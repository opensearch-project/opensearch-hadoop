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

package org.opensearch.hadoop.security;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.opensearch.hadoop.util.ClusterName;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class JdkUserTest {

    @Test
    public void getOpenSearchToken() {
        Subject subject = new Subject();

        String testClusterName = "testClusterName";

        User jdkUser = new JdkUser(subject, new TestSettings());
        assertThat(jdkUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(testClusterName), is(nullValue()));

        OpenSearchToken testToken = new OpenSearchToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken unnamedToken = new OpenSearchToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, OpenSearchMajorVersion.LATEST);

        JdkUser.OpenSearchTokenHolder holder = new JdkUser.OpenSearchTokenHolder();
        holder.setCred(testClusterName, testToken);
        holder.setCred(ClusterName.UNNAMED_CLUSTER_NAME, unnamedToken);
        subject.getPrivateCredentials().add(holder);

        assertThat(jdkUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(testClusterName), is(equalTo(testToken)));
    }

    @Test
    public void addOpenSearchToken() {
        String testClusterName = "testClusterName";

        User jdkUser = new JdkUser(new Subject(), new TestSettings());
        assertThat(jdkUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(testClusterName), is(nullValue()));

        OpenSearchToken testToken = new OpenSearchToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken testToken2 = new OpenSearchToken("zmarx", "pantomime", "pantomime", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken unnamedToken = new OpenSearchToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, OpenSearchMajorVersion.LATEST);

        jdkUser.addOpenSearchToken(testToken);
        jdkUser.addOpenSearchToken(unnamedToken);

        assertThat(jdkUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(testClusterName), is(equalTo(testToken)));

        jdkUser.addOpenSearchToken(testToken2);

        assertThat(jdkUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getOpenSearchToken(testClusterName), is(equalTo(testToken2)));
    }

    @Test
    public void getKerberosPrincipal() {
        Subject subject = new Subject();
        User jdkUser = new JdkUser(subject, new TestSettings());

        assertThat(jdkUser.getKerberosPrincipal(), is(nullValue()));

        KerberosPrincipal principal = new KerberosPrincipal("username@BUILD.CI.OPENSEARCH.ORG");
        subject.getPrincipals().add(principal);

        assertThat(jdkUser.getKerberosPrincipal(), is(equalTo(principal)));
    }
}