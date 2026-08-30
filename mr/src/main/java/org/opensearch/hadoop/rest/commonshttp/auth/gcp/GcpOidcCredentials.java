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

import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;

/**
 * Carries the Google credential provider through the HTTPClient auth machinery so that
 * {@link GcpOidcAuthScheme} can obtain a token when it builds the authorization header.
 *
 * The token itself is deliberately not held here. Google tokens are short lived, so the
 * provider is consulted per request and returns a cached token only while it remains valid.
 */
public class GcpOidcCredentials implements Credentials {

    private final GcpOidcCredentialsProvider provider;

    public GcpOidcCredentials(GcpOidcCredentialsProvider provider) {
        this.provider = provider;
    }

    /**
     * Returns a token that is valid at the time of the call, refreshing if needed.
     *
     * @return the bearer token value
     */
    public String getTokenValue() {
        return provider.getTokenValue();
    }
}
