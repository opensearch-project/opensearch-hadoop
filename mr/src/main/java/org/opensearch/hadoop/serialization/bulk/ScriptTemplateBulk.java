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
package org.opensearch.hadoop.serialization.bulk;

import java.util.Collection;

import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.serialization.builder.ValueWriter;
import org.opensearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.FastByteArrayOutputStream;

class ScriptTemplateBulk extends TemplatedBulk {

    private final Settings settings;

    ScriptTemplateBulk(Settings settings, Collection<Object> beforeObject, Collection<Object> afterObject,
            ValueWriter<?> valueWriter) {
        super(beforeObject, afterObject, valueWriter);
        this.settings = settings;
    }

    @Override
    protected void doWriteObject(Object object, BytesArray storage, ValueWriter<?> writer) {
        if (ConfigurationOptions.OPENSEARCH_OPERATION_UPSERT.equals(settings.getOperation())) {
            if (settings.hasScriptUpsert()) {
                FastByteArrayOutputStream bos = new FastByteArrayOutputStream(storage);
                JacksonJsonGenerator generator = new JacksonJsonGenerator(bos);
                generator.writeBeginObject();
                generator.writeEndObject();
                generator.close();
            } else {
                super.doWriteObject(object, storage, writer);
            }
        }
    }
}