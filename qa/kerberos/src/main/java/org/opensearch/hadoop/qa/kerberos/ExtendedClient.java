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

package org.opensearch.hadoop.qa.kerberos;

import java.io.IOException;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.rest.Response;
import org.opensearch.hadoop.rest.RestClient;
import org.opensearch.hadoop.util.ByteSequence;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.IOUtils;

import static org.opensearch.hadoop.rest.Request.Method.PUT;

public class ExtendedClient extends RestClient {

    public ExtendedClient(Settings settings) {
        super(settings);
    }

    @Override
    public Response execute(Request.Method method, String path, ByteSequence buffer) {
        return super.execute(method, path, buffer);
    }

    public String get(String index) throws IOException {
        return IOUtils.asString(execute(Request.Method.GET, index));
    }

    public String post(String index, byte[] buffer) throws IOException {
        return IOUtils.asString(execute(Request.Method.POST, index, new BytesArray(buffer)).body());
    }

    public String put(String index, byte[] buffer) throws IOException {
        return IOUtils.asString(execute(PUT, index, new BytesArray(buffer)).body());
    }

}