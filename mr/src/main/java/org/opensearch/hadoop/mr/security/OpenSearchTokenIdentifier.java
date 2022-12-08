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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;

import javax.security.auth.Subject;

import org.apache.commons.logging.impl.NoOpLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.cfg.CompositeSettings;
import org.opensearch.hadoop.cfg.HadoopSettingsManager;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.InitializationUtils;
import org.opensearch.hadoop.rest.RestClient;
import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.security.JdkUser;
import org.opensearch.hadoop.security.JdkUserProvider;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.ClusterName;

/**
 * The Hadoop Token Identifier for any generic token that contains an OpenSearchToken within it.
 * <p>
 * Hadoop tokens are generic byte holders that can hold any kind of auth information within them.
 * To identify what specific auth information is within them, they require an "Identifier" to be
 * provided at creation time. This class is used as that identifier.
 * </p>
 * <p>
 * Also included in this class is the Hadoop defined token renewer interface which allows for any
 * process that contains the appropriate service loader info to renew and cancel an existing token.
 * </p>
 * <p>
 * Service loader information is at META-INF/services/org.apache.hadoop.security.token.TokenRenewer
 * and META-INF/services/org.apache.hadoop.security.token.TokenIdentifier
 * </p>
 */
public class OpenSearchTokenIdentifier extends AbstractDelegationTokenIdentifier {

    public static final Text KIND_NAME = new Text("OPENSEARCH_AUTH_TOKEN");

    public static Token<OpenSearchTokenIdentifier> createTokenFrom(OpenSearchToken opensearchToken) {
        OpenSearchTokenIdentifier identifier = new OpenSearchTokenIdentifier();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            opensearchToken.writeOut(new DataOutputStream(buffer));
        } catch (IOException e) {
            throw new OpenSearchHadoopException("Could not serialize token information", e);
        }
        byte[] id = identifier.getBytes();
        byte[] pw = buffer.toByteArray();
        Text kind = identifier.getKind();
        Text service = new Text(opensearchToken.getClusterName());
        return new Token<OpenSearchTokenIdentifier>(id, pw, kind, service);
    }

    @Override
    public Text getKind() {
        return KIND_NAME;
    }

    public static class Renewer extends TokenRenewer {
        @Override
        public boolean handleKind(Text kind) {
            return KIND_NAME.equals(kind);
        }

        @Override
        public boolean isManaged(Token<?> token) throws IOException {
            return true;
        }

        @Override
        public long renew(Token<?> token, Configuration conf) throws IOException, InterruptedException {
            if (!KIND_NAME.equals(token.getKind())) {
                throw new IOException("Could not renew token of invalid type [" + token.getKind().toString() + "]");
            }
            OpenSearchToken opensearchToken = new OpenSearchToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
            return opensearchToken.getExpirationTime();
        }

        @Override
        public void cancel(Token<?> token, Configuration conf) throws IOException, InterruptedException {
            if (!KIND_NAME.equals(token.getKind())) {
                throw new IOException("Could not renew token of invalid type [" + token.getKind().toString() + "]");
            }
            OpenSearchToken opensearchToken = new OpenSearchToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
            Settings settings = HadoopSettingsManager.loadFrom(conf);
            // Create a composite settings object so we can make some changes to the settings without affecting the underlying config
            CompositeSettings compositeSettings = new CompositeSettings(Collections.singletonList(settings));
            // Extract the cluster name from the opensearchToken so that the rest client can locate it for auth purposes
            ClusterInfo info = new ClusterInfo(new ClusterName(opensearchToken.getClusterName(), null), opensearchToken.getMajorVersion());
            compositeSettings.setInternalClusterInfo(info);

            // The RestClient gets the opensearch token for authentication from the current subject, but the subject running this code
            // could be ANYONE. We don't want to just give anyone the token in their credentials, so we create a throw away
            // subject and set it on there. That way we auth with the API key, and once the auth is done, it will be cancelled.
            // We'll do this with the JDK user to avoid the whole Hadoop Library's weird obsession with a static global user subject.
            InitializationUtils.setUserProviderIfNotSet(compositeSettings, JdkUserProvider.class, new NoOpLog());
            Subject subject = new Subject();
            JdkUser user = new JdkUser(subject, settings);
            user.addOpenSearchToken(opensearchToken);
            user.doAs(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    RestClient client = null;
                    try {
                        // TODO: Does not support multiple clusters yet
                        // the client will need to point to the cluster that this token is associated with in order to cancel it.
                        client = createClient(compositeSettings);
                        client.cancelToken(opensearchToken);
                    } finally {
                        if (client != null) {
                            client.close();
                        }
                    }
                    return null;
                }
            });
        }

        // Visible for testing
        protected RestClient createClient(Settings settings) {
            return new RestClient(settings);
        }
    }
}