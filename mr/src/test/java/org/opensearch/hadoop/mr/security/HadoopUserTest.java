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

package org.opensearch.hadoop.mr.security;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.security.User;
import org.opensearch.hadoop.util.ClusterName;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class HadoopUserTest {

    @Test
    public void getOpenSearchToken() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        String testClusterName = "getOpenSearchTokenTest";

        User hadoopUser = new HadoopUser(ugi, new TestSettings());
        assertThat(hadoopUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(testClusterName), is(nullValue()));

        OpenSearchToken testToken = new OpenSearchToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken unnamedToken = new OpenSearchToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, OpenSearchMajorVersion.LATEST);

        OpenSearchTokenIdentifier identifier = new OpenSearchTokenIdentifier();
        byte[] id = identifier.getBytes();
        Text kind = identifier.getKind();

        for (OpenSearchToken token : new OpenSearchToken[]{testToken, unnamedToken}){
            Text service = new Text(token.getClusterName());
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            try {
                token.writeOut(new DataOutputStream(buffer));
            } catch (IOException e) {
                throw new OpenSearchHadoopException("Could not serialize token information", e);
            }
            byte[] pw = buffer.toByteArray();

            ugi.addToken(new Token<OpenSearchTokenIdentifier>(id, pw, kind, service));
        }

        assertThat(hadoopUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(testClusterName), is(equalTo(testToken)));
    }

    @Test
    public void addOpenSearchToken() throws IOException {
        String testClusterName = "addOpenSearchTokenTest";

        User hadoopUser = new HadoopUser(UserGroupInformation.getCurrentUser(), new TestSettings());
        assertThat(hadoopUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(testClusterName), is(nullValue()));

        OpenSearchToken testToken = new OpenSearchToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken testToken2 = new OpenSearchToken("zmarx", "pantomime", "pantomime", System.currentTimeMillis() + 100000L, testClusterName, OpenSearchMajorVersion.LATEST);
        OpenSearchToken unnamedToken = new OpenSearchToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, OpenSearchMajorVersion.LATEST);

        hadoopUser.addOpenSearchToken(testToken);
        hadoopUser.addOpenSearchToken(unnamedToken);

        assertThat(hadoopUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(testClusterName), is(equalTo(testToken)));

        hadoopUser.addOpenSearchToken(testToken2);

        assertThat(hadoopUser.getOpenSearchToken(null), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(""), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(hadoopUser.getOpenSearchToken(testClusterName), is(equalTo(testToken2)));
    }

    @Test
    public void getKerberosPrincipal() throws IOException {
        User jdkUser = new HadoopUser(UserGroupInformation.getCurrentUser(), new TestSettings());
        // This should always be null - We aren't running with Kerberos enabled in this test.
        // See HadoopUserKerberosTest for that.
        assertThat(jdkUser.getKerberosPrincipal(), is(nullValue()));
    }
}