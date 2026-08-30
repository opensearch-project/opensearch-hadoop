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
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_AUDIENCE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_ENABLED;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_SCOPES;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.TestSettings;

import com.google.auth.oauth2.GoogleCredentials;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Exercises both token flows against a local stand-in for Google's token endpoint, using service
 * account credentials so that the real Google auth library path runs: signing the JWT assertion,
 * performing the HTTP exchange, and parsing the JSON response.
 *
 * This module disables transitive dependency resolution (see BuildPlugin), so every dependency the
 * Google auth library needs must be declared explicitly. Tests that stub out the credentials never
 * reach that machinery and so cannot catch a missing dependency; these tests do.
 */
public class GoogleCredentialsTokenSourceHttpTest {

    /** Generated per run so that no private key material is checked in. */
    private static String privateKeyPem;

    @BeforeClass
    public static void generateKey() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        String encoded = Base64.getMimeEncoder(64, new byte[] { '\n' })
                .encodeToString(keyPair.getPrivate().getEncoded());
        privateKeyPem = "-----BEGIN PRIVATE KEY-----\\n" + encoded.replace("\n", "\\n")
                + "\\n-----END PRIVATE KEY-----\\n";
    }

    @Test
    public void fetchesIdTokenOverHttp() throws Exception {
        final String idToken = unsignedJwt();
        HttpServer server = startTokenServer("{\"id_token\":\"" + idToken + "\","
                + "\"access_token\":\"unused\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
        try {
            Settings settings = baseSettings("gcpHttpIdToken");
            settings.setProperty(OPENSEARCH_GCP_OIDC_AUDIENCE, "https://opensearch.example.com");

            GcpToken token = new GoogleCredentialsTokenSource(settings, credentials(server)).fetchToken();

            assertNotNull("Expected an ID token to be resolved", token);
            assertEquals(idToken, token.getValue());
            assertNotNull("Expected an expiry read from the token", token.getExpiresAt());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void fetchesAccessTokenOverHttp() throws Exception {
        HttpServer server = startTokenServer("{\"access_token\":\"real-access-token\","
                + "\"token_type\":\"Bearer\",\"expires_in\":3600}");
        try {
            Settings settings = baseSettings("gcpHttpAccessToken");
            settings.setProperty(OPENSEARCH_GCP_OIDC_TOKEN_TYPE, OPENSEARCH_GCP_OIDC_TOKEN_TYPE_ACCESS_TOKEN);
            settings.setProperty(OPENSEARCH_GCP_OIDC_SCOPES, "https://www.googleapis.com/auth/cloud-platform");

            GcpToken token = new GoogleCredentialsTokenSource(settings, credentials(server)).fetchToken();

            assertNotNull("Expected an access token to be resolved", token);
            assertEquals("real-access-token", token.getValue());
            assertNotNull("Expected an expiry derived from expires_in", token.getExpiresAt());
        } finally {
            server.stop(0);
        }
    }

    private static Settings baseSettings(String name) {
        Settings settings = new TestSettings(name);
        settings.setProperty(OPENSEARCH_GCP_OIDC_ENABLED, "true");
        return settings;
    }

    private static HttpServer startTokenServer(final String responseBody) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/token", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                OutputStream out = exchange.getResponseBody();
                out.write(body);
                out.close();
            }
        });
        server.start();
        return server;
    }

    private static GoogleCredentials credentials(HttpServer server) throws IOException {
        String json = "{\"type\":\"service_account\",\"project_id\":\"test-project\","
                + "\"private_key_id\":\"test-key\",\"private_key\":\"" + privateKeyPem + "\","
                + "\"client_email\":\"test@test-project.iam.gserviceaccount.com\",\"client_id\":\"1\","
                + "\"token_uri\":\"http://localhost:" + server.getAddress().getPort() + "/token\"}";
        return GoogleCredentials.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    /** An unsigned but structurally valid JWT: the library parses the token to read its claims. */
    private static String unsignedJwt() {
        long expiresAt = (System.currentTimeMillis() / 1000L) + 3600L;
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url("{\"aud\":\"https://opensearch.example.com\","
                + "\"iss\":\"https://accounts.google.com\",\"sub\":\"1\",\"exp\":" + expiresAt + "}");
        return header + "." + payload + ".";
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
