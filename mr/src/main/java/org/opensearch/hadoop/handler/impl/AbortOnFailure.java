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

package org.opensearch.hadoop.handler.impl;

import java.util.Properties;

import org.opensearch.hadoop.handler.ErrorCollector;
import org.opensearch.hadoop.handler.ErrorHandler;
import org.opensearch.hadoop.handler.Exceptional;
import org.opensearch.hadoop.handler.HandlerResult;

public class AbortOnFailure<I extends Exceptional, O, C extends ErrorCollector<O>> implements ErrorHandler<I, O, C> {

    public static <I extends Exceptional, O, C extends ErrorCollector<O>> AbortOnFailure<I,O,C> create() {
        return new AbortOnFailure<I, O, C>();
    }

    @Override
    public void init(Properties properties) {}

    @Override
    public void close() {}

    @Override
    public HandlerResult onError(I entry, C collector) throws Exception {
        return HandlerResult.ABORT;
    }
}