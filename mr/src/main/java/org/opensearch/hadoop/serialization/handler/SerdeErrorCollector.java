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

package org.opensearch.hadoop.serialization.handler;

import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;
import org.opensearch.hadoop.handler.ErrorCollector;
import org.opensearch.hadoop.handler.HandlerResult;

/**
 * A generic error collector implementation that can be used for retrying either serialization or deserialization errors.
 */
public class SerdeErrorCollector<T> implements ErrorCollector<T> {
    private boolean isRetry;
    private T currentRetry;
    private String currentMessage;

    @Override
    public HandlerResult retry() {
        isRetry = true;
        return HandlerResult.HANDLED;
    }

    @Override
    public HandlerResult retry(T retryData) {
        currentRetry = retryData;
        return retry();
    }

    @Override
    public HandlerResult pass(String reason) {
        if (currentMessage == null) {
            currentMessage = reason;
        } else {
            throw new OpenSearchHadoopIllegalStateException("Error Handler is attempting to pass with a reason, but a " +
                    "reason already exists! Be sure to return the result of your call to errorCollector.pass(String), " +
                    "and call it only once per call to your Handler!");
        }
        return HandlerResult.PASS;
    }

    public boolean receivedRetries() {
        return isRetry;
    }

    public T getAndClearRetryValue() {
        if (isRetry) {
            T data = null;
            if (currentRetry != null) {
                data = currentRetry;
                currentRetry = null;
            }
            isRetry = false;
            return data;
        } else {
            return null;
        }
    }

    public String getAndClearMessage() {
        String msg = currentMessage;
        currentMessage = null;
        return msg;
    }
}