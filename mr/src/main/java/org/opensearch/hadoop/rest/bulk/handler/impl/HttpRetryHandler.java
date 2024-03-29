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

package org.opensearch.hadoop.rest.bulk.handler.impl;

import java.util.concurrent.TimeUnit;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.handler.HandlerResult;
import org.opensearch.hadoop.rest.HttpRetryPolicy;
import org.opensearch.hadoop.rest.NoHttpRetryPolicy;
import org.opensearch.hadoop.rest.Retry;
import org.opensearch.hadoop.rest.SimpleHttpRetryPolicy;
import org.opensearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.opensearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.opensearch.hadoop.rest.bulk.handler.DelayableErrorCollector;
import org.opensearch.hadoop.util.ObjectUtils;

/**
 * Instantiates the configured HTTP Retry Policy and uses it to determine if a request should be retried.
 *
 * Do not load through the default handler loader, as this requires access to legacy settings.
 */
public class HttpRetryHandler extends BulkWriteErrorHandler {

    public HttpRetryHandler() {
        throw new OpenSearchHadoopIllegalArgumentException("HttpRetryHandler is not loadable through the default handler " +
                "loader logic. Set the HttpRetryPolicy instead.");
    }

    private final Retry retry;
    private int retryLimit;
    private long retryTime;

    public HttpRetryHandler(Settings settings) {
        String retryPolicyName = settings.getBatchWriteRetryPolicy();

        if (ConfigurationOptions.OPENSEARCH_BATCH_WRITE_RETRY_POLICY_SIMPLE.equals(retryPolicyName)) {
            retryPolicyName = SimpleHttpRetryPolicy.class.getName();
        }
        else if (ConfigurationOptions.OPENSEARCH_BATCH_WRITE_RETRY_POLICY_NONE.equals(retryPolicyName)) {
            retryPolicyName = NoHttpRetryPolicy.class.getName();
        }

        HttpRetryPolicy retryPolicy = ObjectUtils.instantiate(retryPolicyName, settings);
        this.retry = retryPolicy.init();

        this.retryLimit = settings.getBatchWriteRetryCount();
        this.retryTime = settings.getBatchWriteRetryWait();
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception {
        if (retry.retry(entry.getResponseCode())) {
            // Negative retry limit? Retry forever.
            if (retryLimit < 0 || entry.getNumberOfAttempts() <= retryLimit) {
                return collector.backoffAndRetry(retryTime, TimeUnit.MILLISECONDS);
            } else {
                return collector.pass("Document bulk write attempts [" + entry.getNumberOfAttempts() +
                        "] exceeds configured automatic retry limit of [" + retryLimit + "]");
            }
        } else {
            return collector.pass("Non retryable code [" + entry.getResponseCode() + "] encountered.");
        }
    }
}