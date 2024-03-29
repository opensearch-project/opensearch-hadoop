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
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.security.User;
import org.opensearch.hadoop.security.UserProvider;
import org.opensearch.hadoop.serialization.OpenSearchHadoopSerializationException;
import org.opensearch.hadoop.util.ClusterName;

/**
 * Provides access to user operations from Hadoop's UserGroupInformation class.
 */
public class HadoopUser implements User {

    private final Settings providerSettings;
    private final UserGroupInformation ugi;

    public HadoopUser(UserGroupInformation ugi, Settings providerSettings) {
        this.ugi = ugi;
        this.providerSettings = providerSettings;
    }

    @Override
    public <T> T doAs(PrivilegedAction<T> action) {
        return ugi.doAs(action);
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws OpenSearchHadoopException {
        try {
            return ugi.doAs(action);
        } catch (IOException e) {
            throw new OpenSearchHadoopException(e);
        } catch (InterruptedException e) {
            throw new OpenSearchHadoopException(e);
        } catch (UndeclaredThrowableException e) {
            throw new OpenSearchHadoopException(e);
        }
    }

    @Override
    public OpenSearchToken getOpenSearchToken(String clusterName) {
        // An unset cluster name - Wouldn't have a token for it.
        if (clusterName == null || clusterName.equals("") || clusterName.equals(ClusterName.UNNAMED_CLUSTER_NAME)) {
            return null;
        }
        for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
            if (OpenSearchTokenIdentifier.KIND_NAME.equals(token.getKind()) && clusterName.equals(token.getService().toString())) {
                try {
                    return new OpenSearchToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
                } catch (IOException e) {
                    throw new OpenSearchHadoopSerializationException("Could not read token information from UGI", e);
                }
            }
        }
        return null; // Token not found
    }

    @Override
    public Iterable<OpenSearchToken> getAllOpenSearchTokens() {
        List<OpenSearchToken> tokens = new ArrayList<>();
        for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
            if (OpenSearchTokenIdentifier.KIND_NAME.equals(token.getKind())) {
                try {
                    tokens.add(new OpenSearchToken(new DataInputStream(new ByteArrayInputStream(token.getPassword()))));
                } catch (IOException e) {
                    throw new OpenSearchHadoopSerializationException("Could not read token information from UGI", e);
                }
            }
        }
        return Collections.unmodifiableList(tokens);
    }

    @Override
    public void addOpenSearchToken(OpenSearchToken opensearchToken) {
        Token<OpenSearchTokenIdentifier> token = OpenSearchTokenIdentifier.createTokenFrom(opensearchToken);
        ugi.addToken(token);
    }

    @Override
    public String getUserName() {
        return ugi.getUserName();
    }

    @Override
    public KerberosPrincipal getKerberosPrincipal() {
        if (ugi.hasKerberosCredentials()) {
            return new KerberosPrincipal(ugi.getUserName());
        }
        return null;
    }

    @Override
    public boolean isProxyUser() {
        return UserGroupInformation.AuthenticationMethod.PROXY.equals(ugi.getAuthenticationMethod());
    }

    @Override
    public UserProvider getRealUserProvider() {
        UserProvider realProvider = new HadoopRealUserProvider();
        realProvider.setSettings(providerSettings);
        return realProvider;
    }

    @Override
    public String toString() {
        return "HadoopUser{" +
                "ugi=" + ugi +
                '}';
    }
}