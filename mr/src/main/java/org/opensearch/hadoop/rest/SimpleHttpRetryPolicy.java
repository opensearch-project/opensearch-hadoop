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
package org.opensearch.hadoop.rest;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.SettingsAware;

public class SimpleHttpRetryPolicy implements HttpRetryPolicy, SettingsAware {

    private class SimpleRetry implements Retry {

        @Override
        public boolean retry(int httpStatus) {
            // everything fine, no need to retry
            if (HttpStatus.isSuccess(httpStatus)) {
                return false;
            }

            switch (httpStatus) {
            // OpenSearch is busy, allow retries
            case HttpStatus.TOO_MANY_REQUESTS:
            case HttpStatus.SERVICE_UNAVAILABLE:
                return true;
            default:
                return false;
            }
        }
    }

    @Override
    public Retry init() {
        return new SimpleRetry();
    }

    @Override
    public void setSettings(Settings settings) {
        // Nothing.
    }
}