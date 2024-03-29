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

package org.opensearch.hadoop.rest.commonshttp.auth.spnego;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.opensearch.hadoop.mr.security.HadoopUserProvider;
import org.opensearch.hadoop.rest.commonshttp.auth.OpenSearchHadoopAuthPolicies;
import org.opensearch.hadoop.security.UgiUtil;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Header;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethodBase;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthChallengeParser;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthChallengeProcessor;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthPolicy;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthScheme;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthState;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.params.HttpClientParams;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.params.HttpParams;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AbstractSpnegoAuthSchemeTest {

    private static File KEYTAB_FILE;

    @BeforeClass
    public static void setUp() throws Exception {
        KEYTAB_FILE = KerberosSuite.getKeytabFile();
    }

    @After
    public void resetUGI() {
        UgiUtil.resetUGI();
    }

    @Test
    public void testAuth() throws Exception {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Execute Test
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(KerberosSuite.PRINCIPAL_CLIENT, KEYTAB_FILE.getAbsolutePath());

        client.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                HttpParams params = new HttpClientParams();

                // Order auth schemes
                OpenSearchHadoopAuthPolicies.registerAuthSchemes();
                List<String> authPreferences = new ArrayList<String>();
                authPreferences.add(OpenSearchHadoopAuthPolicies.NEGOTIATE);
                params.setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPreferences);

                AuthChallengeProcessor authChallengeProcessor = new AuthChallengeProcessor(params);
                TestMethod method = new TestMethod();
                method.setHeaders(new Header[]{new Header("WWW-Authenticate", "Negotiate")});

                Credentials credentials = new SpnegoCredentials(HadoopUserProvider.create(new TestSettings()), KerberosSuite.PRINCIPAL_SERVER);

                // Parse Challenge
                Map challenges = AuthChallengeParser.parseChallenges(method.getResponseHeaders("WWW-Authenticate"));
                assertThat(challenges.isEmpty(), not(true));
                assertThat(challenges.containsKey("negotiate"), is(true));
                assertThat(challenges.get("negotiate"), is("Negotiate"));
                AuthScheme scheme = authChallengeProcessor.processChallenge(method.getHostAuthState(), challenges);

                assertNotNull(scheme);
                assertThat(scheme, instanceOf(SpnegoAuthScheme.class));
                method.getHostAuthState().setAuthAttempted(true);

                // Execute Auth
                Header[] authHeaders = method.getRequestHeaders("Authorization");
                for (Header authHeader : authHeaders) {
                    if (authHeader.isAutogenerated()) {
                        method.removeRequestHeader(authHeader);
                    }
                }
                AuthState authState = method.getHostAuthState();
                AuthScheme authScheme = authState.getAuthScheme();
                assertNotNull(authScheme);
                assertThat(authScheme.isConnectionBased(), is(not(true)));
                String authString = authScheme.authenticate(credentials, method);

                assertNotNull(authString);
                assertThat(authString, startsWith("Negotiate "));
                method.addRequestHeader(new Header("Authorization", authString, true));

                return null;
            }
        });
    }

    @Test
    public void testAuthWithHostBasedServicePrincipal() throws Exception {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Execute Test
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(KerberosSuite.PRINCIPAL_CLIENT, KEYTAB_FILE.getAbsolutePath());

        client.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                HttpParams params = new HttpClientParams();

                // Order auth schemes
                OpenSearchHadoopAuthPolicies.registerAuthSchemes();
                List<String> authPreferences = new ArrayList<String>();
                authPreferences.add(OpenSearchHadoopAuthPolicies.NEGOTIATE);
                params.setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPreferences);

                AuthChallengeProcessor authChallengeProcessor = new AuthChallengeProcessor(params);

                Map<String, String> dnsMappings = new HashMap<String, String>();
                dnsMappings.put("opensearch.build.ci.opensearch.org", "127.0.0.1");

                TestMethod method = new TestMethod();
                method.setHeaders(new Header[]{new Header("WWW-Authenticate", "Negotiate")});
                method.setURI(new org.opensearch.hadoop.thirdparty.apache.commons.httpclient.URI("http", null, "opensearch.build.ci.opensearch.org", 9200));

                Credentials credentials = new SpnegoCredentials(HadoopUserProvider.create(new TestSettings()), "HTTP/_HOST@BUILD.CI.OPENSEARCH.ORG");

                // Parse Challenge
                Map challenges = AuthChallengeParser.parseChallenges(method.getResponseHeaders("WWW-Authenticate"));
                assertThat(challenges.isEmpty(), not(true));
                assertThat(challenges.containsKey("negotiate"), is(true));
                assertThat(challenges.get("negotiate"), is("Negotiate"));
                AuthScheme scheme = authChallengeProcessor.processChallenge(method.getHostAuthState(), challenges);

                assertNotNull(scheme);
                assertThat(scheme, instanceOf(SpnegoAuthScheme.class));
                method.getHostAuthState().setAuthAttempted(true);

                // Execute Auth
                Header[] authHeaders = method.getRequestHeaders("Authorization");
                for (Header authHeader : authHeaders) {
                    if (authHeader.isAutogenerated()) {
                        method.removeRequestHeader(authHeader);
                    }
                }
                AuthState authState = method.getHostAuthState();
                AuthScheme authScheme = authState.getAuthScheme();
                assertNotNull(authScheme);
                assertThat(authScheme.isConnectionBased(), is(not(true)));

                // Replace scheme with test harness scheme
                authScheme = new TestScheme(dnsMappings);
                String authString = authScheme.authenticate(credentials, method);

                assertNotNull(authString);
                assertThat(authString, startsWith("Negotiate "));
                method.addRequestHeader(new Header("Authorization", authString, true));

                return null;
            }
        });
    }


    @Test
    public void testAuthWithReverseLookupServicePrincipal() throws Exception {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Execute Test
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI(KerberosSuite.PRINCIPAL_CLIENT, KEYTAB_FILE.getAbsolutePath());

        client.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                HttpParams params = new HttpClientParams();

                // Order auth schemes
                OpenSearchHadoopAuthPolicies.registerAuthSchemes();
                List<String> authPreferences = new ArrayList<String>();
                authPreferences.add(OpenSearchHadoopAuthPolicies.NEGOTIATE);
                params.setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPreferences);

                AuthChallengeProcessor authChallengeProcessor = new AuthChallengeProcessor(params);

                Map<String, String> dnsMappings = new HashMap<String, String>();
                dnsMappings.put("opensearch.build.ci.opensearch.org", "127.0.0.1");

                TestMethod method = new TestMethod();
                method.setHeaders(new Header[]{new Header("WWW-Authenticate", "Negotiate")});
                method.setURI(new org.opensearch.hadoop.thirdparty.apache.commons.httpclient.URI("http", null, "127.0.0.1", 9200));

                Credentials credentials = new SpnegoCredentials(HadoopUserProvider.create(new TestSettings()), "HTTP/_HOST@BUILD.CI.OPENSEARCH.ORG");

                // Parse Challenge
                Map challenges = AuthChallengeParser.parseChallenges(method.getResponseHeaders("WWW-Authenticate"));
                assertThat(challenges.isEmpty(), not(true));
                assertThat(challenges.containsKey("negotiate"), is(true));
                assertThat(challenges.get("negotiate"), is("Negotiate"));
                AuthScheme scheme = authChallengeProcessor.processChallenge(method.getHostAuthState(), challenges);

                assertNotNull(scheme);
                assertThat(scheme, instanceOf(SpnegoAuthScheme.class));
                method.getHostAuthState().setAuthAttempted(true);

                // Execute Auth
                Header[] authHeaders = method.getRequestHeaders("Authorization");
                for (Header authHeader : authHeaders) {
                    if (authHeader.isAutogenerated()) {
                        method.removeRequestHeader(authHeader);
                    }
                }
                AuthState authState = method.getHostAuthState();
                AuthScheme authScheme = authState.getAuthScheme();
                assertNotNull(authScheme);
                assertThat(authScheme.isConnectionBased(), is(not(true)));

                // Replace scheme with test harness scheme
                authScheme = new TestScheme(dnsMappings);
                String authString = authScheme.authenticate(credentials, method);

                assertNotNull(authString);
                assertThat(authString, startsWith("Negotiate "));
                method.addRequestHeader(new Header("Authorization", authString, true));

                return null;
            }
        });
    }

    private static class TestScheme extends SpnegoAuthScheme {
        private Set<String> dns = new HashSet<>();
        private Map<String, String> rdns = new HashMap<>();

        TestScheme(Map<String, String> nameToIpMap) {
            for (Map.Entry<String, String> entry : nameToIpMap.entrySet()) {
                dns.add(entry.getKey());
                rdns.put(entry.getValue(), entry.getKey());
            }
        }

        @Override
        protected String getFQDN(URI requestURI) throws UnknownHostException {
            if (dns.contains(requestURI.getHost())) {
                return requestURI.getHost();
            } else if (rdns.containsKey(requestURI.getHost())) {
                return rdns.get(requestURI.getHost());
            } else {
                throw new UnknownHostException("Unspecified host [" + requestURI.getHost() + "]");
            }
        }
    }

    private static class TestMethod extends HttpMethodBase {

        void setHeaders(Header[] headers) {
            getResponseHeaderGroup().setHeaders(headers);
        }

        @Override
        public String getName() {
            return "GET";
        }
    }
}