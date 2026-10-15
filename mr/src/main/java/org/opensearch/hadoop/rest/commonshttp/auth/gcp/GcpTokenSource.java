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

/**
 * Resolves Google-issued bearer tokens.
 *
 * Implemented over the Google Auth Library by {@link GoogleCredentialsTokenSource}, and kept as
 * a separate interface so that token caching and refresh behavior can be exercised without
 * live GCP credentials.
 */
public interface GcpTokenSource {

    /**
     * Resolves a fresh token from the underlying credential source.
     *
     * @return a newly fetched token
     * @throws IOException when the credential cannot be resolved or refreshed
     */
    GcpToken fetchToken() throws IOException;
}
