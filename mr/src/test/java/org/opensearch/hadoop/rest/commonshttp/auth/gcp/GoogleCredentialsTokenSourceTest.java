/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.rest.commonshttp.auth.gcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_AUDIENCE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_ENABLED;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_SCOPES;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.TestSettings;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;

/**
 * Exercises the token fetching flows against fake Google credentials, so that the interaction with
 * the Google auth library is covered without needing live credentials.
 *
 * Option validation is covered by
 * {@link org.opensearch.hadoop.rest.InitializationUtilsTest}, which is where it is enforced.
 */
public class GoogleCredentialsTokenSourceTest {

    private static final long ONE_HOUR_MILLIS = 3600L * 1000L;

    private Settings baseSettings() {
        Settings settings = new TestSettings("gcpOidc");
        settings.setProperty(OPENSEARCH_GCP_OIDC_ENABLED, "true");
        return settings;
    }

    /**
     * Builds an unsigned JWT with the given audience and expiry. The Google library parses the ID
     * token to read its claims, so a fake has to supply a structurally valid one.
     */
    private static String jwtFor(String audience, long expiresAtEpochSeconds) {
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url("{\"aud\":\"" + audience + "\",\"iss\":\"https://accounts.google.com\","
                + "\"sub\":\"1234567890\",\"exp\":" + expiresAtEpochSeconds + "}");
        return header + "." + payload + ".";
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Credentials that can mint ID tokens, standing in for a service account. */
    private static class FakeIdTokenCredentials extends GoogleCredentials implements IdTokenProvider {
        private final long expiresAtEpochSeconds;
        private String lastAudience;

        FakeIdTokenCredentials(long expiresAtEpochSeconds) {
            this.expiresAtEpochSeconds = expiresAtEpochSeconds;
        }

        @Override
        public IdToken idTokenWithAudience(String targetAudience, List<IdTokenProvider.Option> options)
                throws IOException {
            this.lastAudience = targetAudience;
            return IdToken.create(jwtFor(targetAudience, expiresAtEpochSeconds));
        }
    }

    /** Credentials that only yield OAuth2 access tokens, and record the scopes requested. */
    private static class FakeAccessTokenCredentials extends GoogleCredentials {
        private final Date expiry;
        private java.util.Collection<String> requestedScopes;

        FakeAccessTokenCredentials(Date expiry) {
            this.expiry = expiry;
        }

        @Override
        public boolean createScopedRequired() {
            return true;
        }

        @Override
        public GoogleCredentials createScoped(java.util.Collection<String> scopes) {
            FakeAccessTokenCredentials scoped = new FakeAccessTokenCredentials(expiry);
            scoped.requestedScopes = scopes;
            return scoped;
        }

        @Override
        public AccessToken refreshAccessToken() {
            return new AccessToken("access-token", expiry);
        }
    }

    @Test
    public void testIdTokenFlowMintsTokenForConfiguredAudience() throws Exception {
        Settings settings = baseSettings();
        settings.setProperty(OPENSEARCH_GCP_OIDC_AUDIENCE, "https://opensearch.example.com");

        long expiresAt = (System.currentTimeMillis() + ONE_HOUR_MILLIS) / 1000L;
        FakeIdTokenCredentials credentials = new FakeIdTokenCredentials(expiresAt);
        GoogleCredentialsTokenSource source = new GoogleCredentialsTokenSource(settings, credentials);

        GcpToken token = source.fetchToken();

        assertNotNull(token);
        assertEquals("https://opensearch.example.com", credentials.lastAudience);
        // The token value is the JWT that OpenSearch will validate.
        assertEquals(jwtFor("https://opensearch.example.com", expiresAt), token.getValue());
        // Expiry comes from the token's own exp claim so refresh is driven by the real lifetime.
        assertEquals(expiresAt, token.getExpiresAt().getEpochSecond());
    }

    @Test
    public void testAccessTokenFlowUsesConfiguredScopes() throws Exception {
        Settings settings = baseSettings();
        settings.setProperty(OPENSEARCH_GCP_OIDC_TOKEN_TYPE, OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN);
        settings.setProperty(OPENSEARCH_GCP_OIDC_SCOPES, "scope/a, scope/b");

        Date expiry = new Date(System.currentTimeMillis() + ONE_HOUR_MILLIS);
        GoogleCredentialsTokenSource source = new GoogleCredentialsTokenSource(settings,
                new FakeAccessTokenCredentials(expiry));

        GcpToken token = source.fetchToken();

        assertNotNull(token);
        assertEquals("access-token", token.getValue());
        assertEquals(expiry.toInstant(), token.getExpiresAt());
    }

    @Test
    public void testIdTokenFlowRejectsCredentialsThatCannotMintIdTokens() {
        Settings settings = baseSettings();
        settings.setProperty(OPENSEARCH_GCP_OIDC_AUDIENCE, "https://opensearch.example.com");

        // Access-token-only credentials cannot mint ID tokens, so this must fail clearly rather
        // than sending something OpenSearch will reject.
        GoogleCredentialsTokenSource source = new GoogleCredentialsTokenSource(settings,
                new FakeAccessTokenCredentials(new Date(System.currentTimeMillis() + ONE_HOUR_MILLIS)));

        try {
            source.fetchToken();
            fail("Expected credentials without ID token support to be rejected");
        } catch (IOException e) {
            fail("Expected a configuration error, got: " + e);
        } catch (RuntimeException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("cannot mint ID tokens"));
        }
    }
}
