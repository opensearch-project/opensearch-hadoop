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

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleResponse implements Response {

    private final int status;
    private final InputStream body;
    private final CharSequence uri;
    private final Map<String, List<String>> headers;

    public SimpleResponse(int status, InputStream body, CharSequence uri) {
        this(status, body, uri, Collections.emptyMap());
    }

    public SimpleResponse(int status, InputStream body, CharSequence uri, Map<String, List<String>> headers) {
        this.status = status;
        this.body = body;
        this.uri = uri;
        this.headers = headers;
    }

    @Override
    public int status() {
        return status;
    }

    public String statusDescription() {
        return HttpStatus.getText(status);
    }

    @Override
    public InputStream body() {
        if (body instanceof ReusableInputStream) {
            InputStream copy = ((ReusableInputStream) body).copy();
            if (copy != null) {
                return copy;
            }
        }
        return body;
    }

    @Override
    public CharSequence uri() {
        return uri;
    }

    @Override
    public boolean isInformal() {
        return HttpStatus.isInformal(status);
    }

    @Override
    public boolean isSuccess() {
        return HttpStatus.isSuccess(status);
    }

    @Override
    public boolean isRedirection() {
        return HttpStatus.isRedirection(status);
    }

    @Override
    public boolean isClientError() {
        return HttpStatus.isClientError(status);
    }

    @Override
    public boolean isServerError() {
        return HttpStatus.isServerError(status);
    }

    @Override
    public boolean hasSucceeded() {
        return isSuccess();
    }

    @Override
    public boolean hasFailed() {
        return !hasSucceeded();
    }

    @Override
    public List<String> getHeaders(String headerName) {
        return headers.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(headerName)).map(Map.Entry::getValue)
                .flatMap(List::stream).collect(Collectors.toList());
    }
}