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

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;

import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;

/**
 * Supplies Google bearer tokens for outgoing requests, caching a resolved token until it
 * approaches expiry.
 *
 * Google tokens are short lived (typically one hour), so a token resolved once at transport
 * construction would break long running batch and structured streaming jobs. Tokens are
 * therefore resolved per request and cached until they fall inside the configured refresh
 * window.
 */
public class GcpOidcCredentialsProvider {

    private final GcpTokenSource tokenSource;
    private final int refreshWindowSeconds;
    private final Clock clock;

    private volatile GcpToken cachedToken;

    public GcpOidcCredentialsProvider(GcpTokenSource tokenSource, int refreshWindowSeconds) {
        this(tokenSource, refreshWindowSeconds, Clock.systemUTC());
    }

    GcpOidcCredentialsProvider(GcpTokenSource tokenSource, int refreshWindowSeconds, Clock clock) {
        this.tokenSource = tokenSource;
        this.refreshWindowSeconds = refreshWindowSeconds;
        this.clock = clock;
    }

    /**
     * Returns a token that is valid for at least the configured refresh window, fetching a new
     * one only when the cached token is absent or close to expiry.
     *
     * @return a usable bearer token value
     */
    public String getTokenValue() {
        GcpToken token = cachedToken;
        Instant now = clock.instant();

        if (token == null || token.needsRefresh(now, refreshWindowSeconds)) {
            synchronized (this) {
                // Re-check under the lock so concurrent callers share a single refresh.
                token = cachedToken;
                now = clock.instant();
                if (token == null || token.needsRefresh(now, refreshWindowSeconds)) {
                    token = refresh();
                    cachedToken = token;
                }
            }
        }

        return token.getValue();
    }

    private GcpToken refresh() {
        GcpToken token;
        try {
            token = tokenSource.fetchToken();
        } catch (IOException e) {
            throw new OpenSearchHadoopIllegalStateException("Could not resolve Google credentials. "
                    + "Ensure Application Default Credentials are available in the environment.", e);
        }
        if (token == null || token.getValue() == null || token.getValue().isEmpty()) {
            throw new OpenSearchHadoopIllegalStateException(
                    "Resolved Google credentials did not yield a usable token value.");
        }
        return token;
    }
}
