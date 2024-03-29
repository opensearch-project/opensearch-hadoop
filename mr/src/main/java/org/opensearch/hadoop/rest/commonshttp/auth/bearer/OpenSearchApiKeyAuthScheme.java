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

package org.opensearch.hadoop.rest.commonshttp.auth.bearer;

import org.apache.commons.codec.binary.Base64;
import org.opensearch.hadoop.rest.commonshttp.auth.OpenSearchHadoopAuthPolicies;
import org.opensearch.hadoop.security.OpenSearchToken;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthenticationException;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.BasicScheme;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.MalformedChallengeException;
import org.opensearch.hadoop.util.StringUtils;

/**
 * Performs authentication by sending an auth header that contains an authentication token.
 *
 * This scheme extends and overrides BasicScheme because HTTPClient 3.0.1 does not allow any
 * preemptive authentication that isn't a subclass of BasicScheme. This allows us to send
 * the auth token up front without waiting to be turned down with a 401 Unauthorized response.
 */
public class OpenSearchApiKeyAuthScheme extends BasicScheme {

    private boolean complete = false;

    @Override
    public boolean isConnectionBased() {
        // Token is sent every request
        return false;
    }

    /**
     * Used to look up the parsed challenges from a request that has returned a 401.
     *
     * @return The scheme name as it appears in the WWW-Authenticate header challenge
     */
    @Override
    public String getSchemeName() {
        return OpenSearchHadoopAuthPolicies.APIKEY;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Called using the challenge text parsed from the header that is associated with this scheme's name.
     * </p>
     */
    @Override
    public void processChallenge(String challenge) throws MalformedChallengeException {
        complete = true;
    }

    /**
     * Implementation method for authentication
     */
    private String authenticate(Credentials credentials) throws AuthenticationException {
        if (!(credentials instanceof OpenSearchApiKeyCredentials)) {
            throw new AuthenticationException("Incorrect credentials type provided. Expected [" + OpenSearchApiKeyCredentials.class.getName()
                    + "] but got [" + credentials.getClass().getName() + "]");
        }

        OpenSearchApiKeyCredentials opensearchApiKeyCredentials = ((OpenSearchApiKeyCredentials) credentials);
        String authString = null;

        if (opensearchApiKeyCredentials.getToken() != null && StringUtils.hasText(opensearchApiKeyCredentials.getToken().getName())) {
            OpenSearchToken token = opensearchApiKeyCredentials.getToken();
            String keyComponents = token.getId() + ":" + token.getApiKey();
            byte[] base64Encoded = Base64.encodeBase64(keyComponents.getBytes(StringUtils.UTF_8));
            String tokenText = new String(base64Encoded, StringUtils.UTF_8);
            authString = OpenSearchHadoopAuthPolicies.APIKEY + " " + tokenText;
        }

        return authString;
    }

    /**
     * Returns the text to send via the Authenticate header on the next request.
     */
    @Override
    public String authenticate(Credentials credentials, HttpMethod method) throws AuthenticationException {
        return authenticate(credentials);
    }

    /**
     * Deprecated method, can still be authenticated with credentials.
     */
    @Override
    public String authenticate(Credentials credentials, String method, String uri) throws AuthenticationException {
        return authenticate(credentials);
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getRealm() {
        // It's not clear what to return here. It seems that null works fine for functional use as it means "Any Realm"
        return null;
    }

    @Override
    public String getParameter(String name) {
        // We don't need no stinkin' parameters
        return null;
    }

    @Override
    public String getID() {
        // Return Scheme Name for maximum bwc safety.
        return getSchemeName();
    }
}