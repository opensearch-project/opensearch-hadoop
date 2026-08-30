/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.rest.commonshttp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_AUDIENCE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_ENABLED;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_REFRESH_WINDOW;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_NET_HTTP_AUTH_USER;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request.Method;
import org.opensearch.hadoop.rest.SimpleRequest;
import org.opensearch.hadoop.security.SecureSettings;
import org.opensearch.hadoop.rest.commonshttp.auth.gcp.GcpToken;
import org.opensearch.hadoop.rest.commonshttp.auth.gcp.GcpTokenSource;
import org.opensearch.hadoop.util.TestSettings;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Verifies that Google OIDC authentication reaches the wire through the transport.
 *
 * The vendored HTTPClient 3.1 only sends credentials preemptively for the scheme registered under
 * {@code AuthState.PREEMPTIVE_AUTH_SCHEME}, which is hard coded to basic. Registering a custom
 * scheme with an auth policy and credentials is therefore not by itself enough to put a token on
 * the request; the scheme has to be seeded onto the method's auth state as well. These tests pin
 * that behaviour by asserting on what the server actually received, including that no extra
 * challenge round trip is needed.
 */
public class CommonsHttpTransportGcpAuthTest {

    private HttpServer server;
    private final List<String> observedAuthHeaders = Collections.synchronizedList(new ArrayList<String>());

    /** Hands out a distinct token per fetch so a stale token is detectable. */
    private static class CountingTokenSource implements GcpTokenSource {
        private final long lifetimeSeconds;
        private int fetches = 0;

        CountingTokenSource(long lifetimeSeconds) {
            this.lifetimeSeconds = lifetimeSeconds;
        }

        @Override
        public GcpToken fetchToken() throws IOException {
            fetches++;
            return new GcpToken("token-" + fetches, Instant.now().plusSeconds(lifetimeSeconds));
        }
    }

    @Before
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                observedAuthHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
                byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        server.start();
    }

    @After
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private String host() {
        return "127.0.0.1:" + server.getAddress().getPort();
    }

    private Settings baseSettings(String name) {
        Settings settings = new TestSettings(name);
        settings.setProperty(OPENSEARCH_GCP_OIDC_ENABLED, "true");
        settings.setProperty(OPENSEARCH_GCP_OIDC_AUDIENCE, "https://opensearch.example.com");
        return settings;
    }

    private void get(Settings settings, GcpTokenSource tokenSource) throws IOException {
        CommonsHttpTransport transport = new CommonsHttpTransport(settings, new SecureSettings(settings), host(), tokenSource);
        try {
            transport.execute(new SimpleRequest(Method.GET, null, "/"));
        } finally {
            transport.close();
        }
    }

    @Test
    public void testTokenIsSentPreemptively() throws Exception {
        get(baseSettings("gcpTransportPreemptive"), new CountingTokenSource(3600));

        // A single request: no 401 challenge round trip.
        assertEquals(1, observedAuthHeaders.size());
        assertEquals("Bearer token-1", observedAuthHeaders.get(0));
    }

    @Test
    public void testTokenIsReusedAcrossRequestsWhileValid() throws Exception {
        Settings settings = baseSettings("gcpTransportReuse");
        CountingTokenSource tokenSource = new CountingTokenSource(3600);

        CommonsHttpTransport transport = new CommonsHttpTransport(settings, new SecureSettings(settings), host(), tokenSource);
        try {
            transport.execute(new SimpleRequest(Method.GET, null, "/"));
            transport.execute(new SimpleRequest(Method.GET, null, "/"));
        } finally {
            transport.close();
        }

        assertEquals(2, observedAuthHeaders.size());
        assertEquals("Bearer token-1", observedAuthHeaders.get(0));
        assertEquals("Bearer token-1", observedAuthHeaders.get(1));
        assertEquals("Only one token should have been minted", 1, tokenSource.fetches);
    }

    @Test
    public void testTokenIsRefreshedWhenInsideRefreshWindow() throws Exception {
        Settings settings = baseSettings("gcpTransportRefresh");
        // A lifetime shorter than the refresh window means every request needs a fresh token,
        // which is what keeps a long running job authenticated past the token lifetime.
        settings.setProperty(OPENSEARCH_GCP_OIDC_TOKEN_REFRESH_WINDOW, "300");
        CountingTokenSource tokenSource = new CountingTokenSource(10);

        CommonsHttpTransport transport = new CommonsHttpTransport(settings, new SecureSettings(settings), host(), tokenSource);
        try {
            transport.execute(new SimpleRequest(Method.GET, null, "/"));
            transport.execute(new SimpleRequest(Method.GET, null, "/"));
        } finally {
            transport.close();
        }

        assertEquals(2, observedAuthHeaders.size());
        assertEquals("Bearer token-1", observedAuthHeaders.get(0));
        assertEquals("Bearer token-2", observedAuthHeaders.get(1));
    }

    @Test
    public void testGoogleTokenTakesPrecedenceOverBasicAuth() throws Exception {
        Settings settings = baseSettings("gcpTransportVsBasic");
        settings.setProperty(OPENSEARCH_NET_HTTP_AUTH_USER, "someuser");

        get(settings, new CountingTokenSource(3600));

        // Both schemes own the Authorization header. The Google token must win outright rather
        // than the two silently overwriting each other.
        assertEquals(1, observedAuthHeaders.size());
        assertEquals("Bearer token-1", observedAuthHeaders.get(0));
    }

    @Test
    public void testBasicAuthIsUnaffectedWhenDisabled() throws Exception {
        // The shared test properties configure basic auth, so this also guards against the Google
        // wiring taking over the Authorization header when it has not been asked for.
        Settings settings = new TestSettings("gcpTransportDisabled");

        get(settings, new CountingTokenSource(3600));

        assertEquals(1, observedAuthHeaders.size());
        String header = observedAuthHeaders.get(0);
        assertNotNull("Basic auth should still be sent", header);
        assertTrue("Expected basic auth, got: " + header, header.startsWith("Basic "));
    }
}
