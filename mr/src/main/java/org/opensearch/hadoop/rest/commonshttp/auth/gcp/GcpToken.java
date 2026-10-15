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

import java.time.Instant;

/**
 * An immutable Google-issued bearer token along with the instant at which it expires.
 *
 * A null expiry means the token carries no expiration information, in which case it is
 * treated as always in need of a refresh so that a fresh credential is resolved per request.
 */
public class GcpToken {

    private final String value;
    private final Instant expiresAt;

    public GcpToken(String value, Instant expiresAt) {
        this.value = value;
        this.expiresAt = expiresAt;
    }

    public String getValue() {
        return value;
    }

    /**
     * @return the expiry instant, or null when the token source did not report one.
     */
    public Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * Determines whether this token should be refreshed, treating a token that expires within
     * refreshWindowSeconds as already expired so that a long request does not outlive it.
     *
     * @param now the current instant
     * @param refreshWindowSeconds seconds before actual expiry at which to force a refresh
     * @return true when the token must be refreshed before use
     */
    public boolean needsRefresh(Instant now, int refreshWindowSeconds) {
        if (expiresAt == null) {
            return true;
        }
        return !now.plusSeconds(refreshWindowSeconds).isBefore(expiresAt);
    }
}
