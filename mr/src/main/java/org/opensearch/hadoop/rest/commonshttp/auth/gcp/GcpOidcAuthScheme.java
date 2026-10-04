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

import org.opensearch.hadoop.rest.commonshttp.auth.OpenSearchHadoopAuthPolicies;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthenticationException;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.BasicScheme;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.auth.MalformedChallengeException;

/**
 * Performs authentication by sending a Google issued token in the authorization header.
 *
 * Like {@link org.opensearch.hadoop.rest.commonshttp.auth.bearer.OpenSearchApiKeyAuthScheme}, this
 * extends BasicScheme because HTTPClient 3.0.1 only allows preemptive authentication for subclasses
 * of BasicScheme, which lets the token be sent up front rather than after a 401.
 */
public class GcpOidcAuthScheme extends BasicScheme {

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
        return OpenSearchHadoopAuthPolicies.BEARER;
    }

    @Override
    public void processChallenge(String challenge) throws MalformedChallengeException {
        complete = true;
    }

    private String authenticate(Credentials credentials) throws AuthenticationException {
        if (!(credentials instanceof GcpOidcCredentials)) {
            throw new AuthenticationException("Incorrect credentials type provided. Expected ["
                    + GcpOidcCredentials.class.getName() + "] but got [" + credentials.getClass().getName() + "]");
        }

        // Resolved per request so that a token which expires mid job is refreshed rather than reused.
        String token = ((GcpOidcCredentials) credentials).getTokenValue();
        return OpenSearchHadoopAuthPolicies.BEARER + " " + token;
    }

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
        // Null means "any realm", consistent with the other custom schemes here.
        return null;
    }

    @Override
    public String getParameter(String name) {
        return null;
    }

    @Override
    public String getID() {
        // Return Scheme Name for maximum bwc safety.
        return getSchemeName();
    }
}
