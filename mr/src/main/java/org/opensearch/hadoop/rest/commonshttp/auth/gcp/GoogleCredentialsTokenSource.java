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

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.StringUtils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;

/**
 * Resolves Google tokens from Application Default Credentials.
 *
 * Two flows are supported, selected by {@code opensearch.gcp.oidc.token.type}:
 * <ul>
 *     <li>{@code id_token} (default): mints an OIDC ID token for a configured audience, validated
 *     by the OpenSearch security plugin's OpenID Connect / JWT backend.</li>
 *     <li>{@code access_token}: an OAuth2 access token for configured scopes, for deployments that
 *     front OpenSearch with an authenticating proxy.</li>
 * </ul>
 *
 * The options themselves are validated by
 * {@link org.opensearch.hadoop.rest.InitializationUtils#validateSettings} so that a
 * misconfiguration is reported before any work starts.
 */
public class GoogleCredentialsTokenSource implements GcpTokenSource {

    private final String tokenType;
    private final String audience;
    private final Set<String> scopes;
    private final GoogleCredentials baseCredentials;

    public GoogleCredentialsTokenSource(Settings settings) {
        this(settings, null);
    }

    GoogleCredentialsTokenSource(Settings settings, GoogleCredentials baseCredentials) {
        this.tokenType = settings.getGcpOidcTokenType();
        this.audience = settings.getGcpOidcAudience();
        this.scopes = parseScopes(settings.getGcpOidcScopes());
        this.baseCredentials = baseCredentials;
    }

    private static Set<String> parseScopes(String rawScopes) {
        Set<String> parsed = new LinkedHashSet<String>();
        if (StringUtils.hasText(rawScopes)) {
            for (String scope : Arrays.asList(rawScopes.split(","))) {
                String trimmed = scope.trim();
                if (!trimmed.isEmpty()) {
                    parsed.add(trimmed);
                }
            }
        }
        return parsed;
    }

    @Override
    public GcpToken fetchToken() throws IOException {
        GoogleCredentials credentials = baseCredentials != null
                ? baseCredentials
                : GoogleCredentials.getApplicationDefault();

        if (ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ID_TOKEN.equals(tokenType)) {
            return fetchIdToken(credentials);
        } else if (ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN.equals(tokenType)) {
            return fetchAccessToken(credentials);
        }
        // Normally unreachable: InitializationUtils rejects unsupported token types up front.
        throw new OpenSearchHadoopIllegalArgumentException("Unsupported ["
                + ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE + "] value [" + tokenType + "]");
    }

    private GcpToken fetchIdToken(GoogleCredentials credentials) throws IOException {
        if (!(credentials instanceof IdTokenProvider)) {
            throw new OpenSearchHadoopIllegalArgumentException("The resolved Google credentials of type ["
                    + credentials.getClass().getName() + "] cannot mint ID tokens. Use a service account "
                    + "credential or a workload identity enabled environment, or switch ["
                    + ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE + "] to ["
                    + ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN + "]");
        }

        IdTokenCredentials idTokenCredentials = IdTokenCredentials.newBuilder()
                .setIdTokenProvider((IdTokenProvider) credentials)
                .setTargetAudience(audience)
                .build();

        idTokenCredentials.refreshIfExpired();
        return toGcpToken(idTokenCredentials.getAccessToken());
    }

    private GcpToken fetchAccessToken(GoogleCredentials credentials) throws IOException {
        GoogleCredentials scoped = credentials.createScoped(scopes);
        scoped.refreshIfExpired();
        return toGcpToken(scoped.getAccessToken());
    }

    private static GcpToken toGcpToken(AccessToken accessToken) {
        if (accessToken == null) {
            return null;
        }
        Date expiration = accessToken.getExpirationTime();
        Instant expiresAt = expiration != null ? expiration.toInstant() : null;
        return new GcpToken(accessToken.getTokenValue(), expiresAt);
    }
}
