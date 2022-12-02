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

package org.opensearch.hadoop.serialization.handler.read.impl;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.handler.ErrorCollector;
import org.opensearch.hadoop.handler.ErrorHandler;
import org.opensearch.hadoop.handler.impl.AbortOnFailure;
import org.opensearch.hadoop.handler.impl.AbstractHandlerLoader;
import org.opensearch.hadoop.handler.impl.DropAndLog;
import org.opensearch.hadoop.handler.impl.opensearch.OpenSearchHandler;
import org.opensearch.hadoop.serialization.handler.read.DeserializationFailure;
import org.opensearch.hadoop.serialization.handler.read.IDeserializationErrorHandler;

public class DeserializationHandlerLoader extends AbstractHandlerLoader<IDeserializationErrorHandler> {

    public static final String OPENSEARCH_READ_DATA_ERROR_HANDLERS = "opensearch.read.data.error.handlers";
    public static final String OPENSEARCH_READ_DATA_ERROR_HANDLER = "opensearch.read.data.error.handler";

    public DeserializationHandlerLoader() {
        super(IDeserializationErrorHandler.class);
    }

    @Override
    protected String getHandlersPropertyName() {
        return OPENSEARCH_READ_DATA_ERROR_HANDLERS;
    }

    @Override
    protected String getHandlerPropertyName() {
        return OPENSEARCH_READ_DATA_ERROR_HANDLER;
    }

    @Override
    protected IDeserializationErrorHandler loadBuiltInHandler(AbstractHandlerLoader.NamedHandlers handlerName) {
        ErrorHandler<DeserializationFailure, byte[], ErrorCollector<byte[]>> genericHandler;
        switch (handlerName) {
            case FAIL:
                genericHandler = AbortOnFailure.create();
                break;
            case LOG:
                genericHandler = DropAndLog.create(new DeserializationLogRenderer());
                break;
            case OPENSEARCH:
                genericHandler = OpenSearchHandler.create(getSettings(), new DeserializationEventConverter());
                break;
            default:
                throw new OpenSearchHadoopIllegalArgumentException(
                        "Could not find default implementation for built in handler type [" + handlerName + "]"
                );
        }
        return new DelegatingErrorHandler(genericHandler);
    }
}