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
package org.opensearch.storm.serialization;

import org.apache.storm.tuple.Tuple;
import org.opensearch.hadoop.serialization.JdkBytesConverter;
import org.opensearch.hadoop.util.Assert;
import org.opensearch.hadoop.util.BytesArray;

public class StormTupleBytesConverter extends JdkBytesConverter {

    @Override
    public void convert(Object from, BytesArray to) {
        Assert.isTrue(from == null || from instanceof Tuple,
                String.format("Unexpected object type, expecting [%s], given [%s]", Tuple.class, from.getClass()));

        // handle common cases
        Tuple tuple = (Tuple) from;

        if (tuple == null || tuple.size() == 0) {
            to.bytes("{}");
            return;
        }
        Assert.isTrue(tuple.size() == 1, "When using JSON input, only one field is expected");

        super.convert(tuple.getValue(0), to);
    }
}