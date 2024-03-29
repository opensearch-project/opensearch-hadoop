/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
 
/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.hadoop.integration.rest.ssl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.cfg.PropertiesSettings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.rest.Request.Method;
import org.opensearch.hadoop.rest.Response;
import org.opensearch.hadoop.rest.SimpleRequest;
import org.opensearch.hadoop.rest.commonshttp.CommonsHttpTransport;
import org.opensearch.hadoop.rest.ssl.BasicSSLServer;
import org.opensearch.hadoop.security.KeystoreWrapper;
import org.opensearch.hadoop.security.SecureSettings;
import org.opensearch.hadoop.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.*;

public class SSLTests {

    private static int SSL_PORT;
    private final String PREFIX = "prefixed_path";

    @ClassRule
    public static ExternalResource SSL_SERVER = new ExternalResource() {
        private BasicSSLServer server;

        @Override
        protected void before() throws Throwable {
            Properties props = HdpBootstrap.asProperties(HdpBootstrap.hadoopConfig());
            SSL_PORT = Integer.parseInt(props.getProperty("test.ssl.port", "12345"));

            server = new BasicSSLServer(SSL_PORT);
            server.start();
        }

        @Override
        protected void after() {
            try {
                server.stop();
            } catch (Exception ex) {
            }
        }
    };

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private PropertiesSettings cfg;
    private CommonsHttpTransport transport;

    @Before
    public void setup() throws Exception {
        KeystoreWrapper keystoreWrapper = KeystoreWrapper.newStore().build();
        keystoreWrapper.setSecureSetting(OPENSEARCH_NET_SSL_TRUST_STORE_PASS, "testpass");
        File ks = TEMP_FOLDER.newFile();
        OutputStream ksOut = new FileOutputStream(ks);
        keystoreWrapper.saveKeystore(ksOut);
        ksOut.close();

        cfg = new PropertiesSettings();
        cfg.setPort(SSL_PORT);
        cfg.setProperty(OPENSEARCH_KEYSTORE_LOCATION, ks.toURI().toString());
        cfg.setProperty(OPENSEARCH_NET_USE_SSL, "true");
        cfg.setProperty(OPENSEARCH_NET_SSL_CERT_ALLOW_SELF_SIGNED, "true");
        cfg.setProperty(OPENSEARCH_NET_SSL_TRUST_STORE_LOCATION, "ssl/client.jks");
        cfg.setProperty(OPENSEARCH_NODES_PATH_PREFIX, PREFIX);

        transport = new CommonsHttpTransport(cfg.copy(), new SecureSettings(cfg), "localhost");
    }

    @After
    public void destroy() {
        transport.close();
    }

    @Test
    public void testBasicSSLHandshake() throws Exception {
        String uri = "localhost:" + SSL_PORT;
        String path = "/basicSSL";
        Request req = new SimpleRequest(Method.GET, uri, path);

        Response execute = transport.execute(req);
        String content = IOUtils.asString(execute.body());
        assertEquals("/" + PREFIX + path, content);
    }

    @Test
    public void testBasicSSLHandshakeWithSchema() throws Exception {
        String uri = "https://localhost:" + SSL_PORT;
        String path = "/basicSSL";
        Request req = new SimpleRequest(Method.GET, uri, path);

        Response execute = transport.execute(req);
        String content = IOUtils.asString(execute.body());
        assertEquals("/" + PREFIX + path, content);
    }
}