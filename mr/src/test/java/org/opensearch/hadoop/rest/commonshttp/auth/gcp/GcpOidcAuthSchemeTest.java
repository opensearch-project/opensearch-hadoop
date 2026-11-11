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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Instant;

import org.junit.Test;
import org.opensearch.hadoop.rest.commonshttp.auth.OpenSearchHadoopAuthPolicies;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.UsernamePasswordCredentials;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthenticationException;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.methods.GetMethod;

/**
 * Verifies the authorization header that the scheme produces, and that it asks the provider for a
 * token on every call rather than capturing one when the scheme is created.
 */
public class GcpOidcAuthSchemeTest {

    private static final int REFRESH_WINDOW = 300;

    /** Hands out a new token value on each fetch, so a stale value is detectable. */
    private static class CountingTokenSource implements GcpTokenSource {
        private final Instant expiresAt;
        private int fetches = 0;

        CountingTokenSource(Instant expiresAt) {
            this.expiresAt = expiresAt;
        }

        @Override
        public GcpToken fetchToken() throws IOException {
            fetches++;
            return new GcpToken("token-" + fetches, expiresAt);
        }
    }

    @Test
    public void testSchemeNameIsBearer() {
        assertEquals(OpenSearchHadoopAuthPolicies.BEARER, new GcpOidcAuthScheme().getSchemeName());
        // Sent on every request rather than bound to a single connection.
        assertFalse(new GcpOidcAuthScheme().isConnectionBased());
    }

    @Test
    public void testProducesBearerAuthorizationHeader() throws Exception {
        CountingTokenSource tokenSource = new CountingTokenSource(Instant.now().plusSeconds(3600));
        GcpOidcCredentials credentials = new GcpOidcCredentials(
                new GcpOidcCredentialsProvider(tokenSource, REFRESH_WINDOW));

        String header = new GcpOidcAuthScheme().authenticate(credentials, new GetMethod("/"));

        assertEquals("Bearer token-1", header);
    }

    @Test
    public void testTokenIsResolvedPerCallAndRefreshedWhenExpired() throws Exception {
        // Already inside the refresh window, so every call must go back to the provider.
        CountingTokenSource tokenSource = new CountingTokenSource(Instant.now().plusSeconds(10));
        GcpOidcCredentials credentials = new GcpOidcCredentials(
                new GcpOidcCredentialsProvider(tokenSource, REFRESH_WINDOW));
        GcpOidcAuthScheme scheme = new GcpOidcAuthScheme();

        String first = scheme.authenticate(credentials, new GetMethod("/"));
        String second = scheme.authenticate(credentials, new GetMethod("/"));

        assertEquals("Bearer token-1", first);
        // A scheme that cached the token would repeat the first value here, which is what would
        // break a job running longer than the token lifetime.
        assertEquals("Bearer token-2", second);
    }

    @Test
    public void testCachedTokenIsReusedWhileStillValid() throws Exception {
        CountingTokenSource tokenSource = new CountingTokenSource(Instant.now().plusSeconds(3600));
        GcpOidcCredentials credentials = new GcpOidcCredentials(
                new GcpOidcCredentialsProvider(tokenSource, REFRESH_WINDOW));
        GcpOidcAuthScheme scheme = new GcpOidcAuthScheme();

        scheme.authenticate(credentials, new GetMethod("/"));
        scheme.authenticate(credentials, new GetMethod("/"));

        // Still well outside the refresh window, so only one token should have been minted.
        assertEquals(1, tokenSource.fetches);
    }

    @Test
    public void testRejectsUnexpectedCredentialsType() {
        try {
            new GcpOidcAuthScheme().authenticate(new UsernamePasswordCredentials("user", "pass"),
                    new GetMethod("/"));
            fail("Expected non-Google credentials to be rejected");
        } catch (AuthenticationException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains(GcpOidcCredentials.class.getName()));
        }
    }
}
