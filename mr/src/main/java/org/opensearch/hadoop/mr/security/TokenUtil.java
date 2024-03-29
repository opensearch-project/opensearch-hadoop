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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.token.Token;
import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.rest.RestClient;
import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.security.User;
import org.opensearch.hadoop.util.Assert;
import org.opensearch.hadoop.util.ClusterName;

/**
 * Provides the main logic for obtaining Hadoop specific Tokens containing OpenSearch authentication
 * info, caching them in User objects (Subjects or Hadoop UGI), and adding them to jobs and job configurations.
 */
public class TokenUtil {

    private static Log LOG = LogFactory.getLog(TokenUtil.class);

    public static final String KEY_NAME_PREFIX = "OPENSEARCHHADOOP_";

    /**
     * Generates a new unique name for an API Key
     * @return unique api key name
     */
    private static String newKeyName() {
        return KEY_NAME_PREFIX + UUID.randomUUID().toString();
    }

    /**
     * Obtain the given user's authentication token from OpenSearch by performing the getAuthToken operation
     * as the given user, thus ensuring the subject's private credentials available on the thread's access control
     * context for the life of the operation.
     * @param client The OpenSearch client
     * @param user the user object that contains credentials for obtaining an auth token
     * @return the authentication token in OpenSearch-Hadoop specific format.
     */
    private static OpenSearchToken obtainOpenSearchToken(final RestClient client, User user) {
        // TODO: Should we extend this to basic authentication at some point?
        KerberosPrincipal principal = user.getKerberosPrincipal();
        if (user.isProxyUser()) {
            principal = user.getRealUserProvider().getUser().getKerberosPrincipal();
        }
        Assert.isTrue(principal != null, "Kerberos credentials are missing on current user");
        return user.doAs(new PrivilegedExceptionAction<OpenSearchToken>() {
            @Override
            public OpenSearchToken run() {
                return client.createNewApiToken(newKeyName());
            }
        });
    }

    /**
     * Obtain and return an authentication token for the current user.
     * @param client The OpenSearch client
     * @return the authentication token instance in Hadoop Token format.
     */
    public static Token<OpenSearchTokenIdentifier> obtainToken(RestClient client, User user) {
        OpenSearchToken opensearchToken = obtainOpenSearchToken(client, user);
        return OpenSearchTokenIdentifier.createTokenFrom(opensearchToken);
    }

    /**
     * Obtain an authentication token for the given user and add it to the
     * user's credentials.
     * @param client The OpenSearch client
     * @param user The user for obtaining and storing the token
     * @throws IOException If making a remote call to the authentication service fails
     */
    public static void obtainAndCache(RestClient client, User user) throws IOException {
        OpenSearchToken token = obtainOpenSearchToken(client, user);

        if (token == null) {
            throw new IOException("No token returned for user " + user.getKerberosPrincipal().getName());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtained token " + OpenSearchTokenIdentifier.KIND_NAME + " for user " +
                    user.getKerberosPrincipal().getName());
        }
        user.addOpenSearchToken(token);
    }

    /**
     * Obtain an authentication token on behalf of the given user and add it to
     * the credentials for the given map reduce job. This version always obtains
     * a fresh authentication token instead of checking for existing ones on the
     * current user.
     *
     * @param client The OpenSearch client
     * @param user The user for whom to obtain the token
     * @param job The job instance in which the token should be stored
     */
    public static void obtainTokenForJob(final RestClient client, User user, Job job) {
        Token<OpenSearchTokenIdentifier> token = obtainToken(client, user);
        if (token == null) {
            throw new OpenSearchHadoopException("No token returned for user " + user.getKerberosPrincipal().getName());
        }
        Text clusterName = token.getService();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtained token " + OpenSearchTokenIdentifier.KIND_NAME.toString() + " for user " +
                    user.getKerberosPrincipal().getName() + " on cluster " + clusterName.toString());
        }
        job.getCredentials().addToken(clusterName, token);
    }

    /**
     * Obtain an authentication token on behalf of the given user and add it to
     * the credentials for the given map reduce job. This version always obtains
     * a fresh authentication token instead of checking for existing ones on the
     * current user.
     *
     * @param client The OpenSearch client
     * @param user The user for whom to obtain the token
     * @param jobConf The job configuration in which the token should be stored
     */
    public static void obtainTokenForJob(final RestClient client, User user, JobConf jobConf) {
        Token<OpenSearchTokenIdentifier> token = obtainToken(client, user);
        if (token == null) {
            throw new OpenSearchHadoopException("No token returned for user " + user.getKerberosPrincipal().getName());
        }
        Text clusterName = token.getService();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Obtained token " + OpenSearchTokenIdentifier.KIND_NAME.toString() + " for user " +
                    user.getKerberosPrincipal().getName() + " on cluster " + clusterName.toString());
        }
        jobConf.getCredentials().addToken(clusterName, token);
    }

    /**
     * Retrieves an authentication token from the given user, obtaining a new token if necessary,
     * and adds it to the credentials for the given map reduce job.
     *
     * @param client The OpenSearch client
     * @param clusterName the name of the cluster you are connecting to
     * @param user The user for whom to obtain the token
     * @param job The job instance in which the token should be stored
     * @throws IOException If making a remote call fails
     * @throws InterruptedException If executing as the given user is interrupted
     */
    public static void addTokenForJob(final RestClient client, ClusterName clusterName, User user, Job job) {
        Token<OpenSearchTokenIdentifier> token = getAuthToken(clusterName, user);
        if (token == null) {
            token = obtainToken(client, user);
        }
        job.getCredentials().addToken(token.getService(), token);
    }

    /**
     * Retrieves an authentication token from the given user, obtaining a new token if necessary,
     * and adds it to the credentials for the given map reduce job configuration.
     *
     * @param client The OpenSearch client
     * @param clusterName the name of the cluster you are connecting to
     * @param user The user for whom to obtain the token
     * @param job The job instance in which the token should be stored
     * @throws IOException If making a remote call fails
     * @throws InterruptedException If executing as the given user is interrupted
     */
    public static void addTokenForJobConf(final RestClient client, ClusterName clusterName, User user, JobConf job) {
        Token<OpenSearchTokenIdentifier> token = getAuthToken(clusterName, user);
        if (token == null) {
            token = obtainToken(client, user);
        }
        job.getCredentials().addToken(token.getService(), token);
    }

    /**
     * Get the authentication token of the user for the provided cluster name in its Hadoop Token form.
     * @return null if the user does not have the token, otherwise the auth token for the cluster.
     */
    private static Token<OpenSearchTokenIdentifier> getAuthToken(ClusterName clusterName, User user) {
        OpenSearchToken opensearchToken = getOpenSearchAuthToken(clusterName, user);
        if (opensearchToken == null) {
            return null;
        } else {
            return OpenSearchTokenIdentifier.createTokenFrom(opensearchToken);
        }
    }

    /**
     * Get the authentication token of the user for the provided cluster name in its OpenSearch-Hadoop specific form.
     * @return null if the user does not have the token, otherwise the auth token for the cluster.
     */
    private static OpenSearchToken getOpenSearchAuthToken(ClusterName clusterName, User user) {
        return user.getOpenSearchToken(clusterName.getName());
    }
}