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
import java.util.List;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.serialization.builder.ContentBuilder;
import org.opensearch.hadoop.serialization.builder.ValueWriter;
import org.opensearch.hadoop.serialization.bulk.AbstractBulkFactory.DynamicContentRef;
import org.opensearch.hadoop.serialization.bulk.AbstractBulkFactory.FieldWriter;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.BytesRef;
import org.opensearch.hadoop.util.FastByteArrayOutputStream;

class TemplatedBulk implements BulkCommand {

    protected final Collection<Object> beforeObject;
    protected final Collection<Object> afterObject;

    protected BytesArray scratchPad = new BytesArray(1024);
    protected BytesRef ref = new BytesRef();

    private final ValueWriter valueWriter;

    TemplatedBulk(Collection<Object> beforeObject, Collection<Object> afterObject, ValueWriter<?> valueWriter) {
        this.beforeObject = beforeObject;
        this.afterObject = afterObject;
        this.valueWriter = valueWriter;
    }

    @Override
    public BytesRef write(Object object) {
        ref.reset();
        scratchPad.reset();

        Object processed = preProcess(object, scratchPad);
        // write before object
        writeTemplate(beforeObject, processed);
        // write object
        doWriteObject(processed, scratchPad, valueWriter);
        ref.add(scratchPad);
        // writer after object
        writeTemplate(afterObject, processed);
        return ref;
    }

    protected Object preProcess(Object object, BytesArray storage) {
        return object;
    }

    protected void doWriteObject(Object object, BytesArray storage, ValueWriter<?> writer) {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(storage);
        ContentBuilder.generate(bos, writer).value(object).flush().close();
    }

    protected void writeTemplate(Collection<Object> template, Object object) {
        for (Object item : template) {
            if (item instanceof BytesArray) {
                ref.add((BytesArray) item);
            }
            else if (item instanceof FieldWriter) {
                ref.add(((FieldWriter) item).write(object));
            }
            // used in the dynamic case
            else if (item instanceof DynamicContentRef) {
                List<Object> dynamicContent = ((DynamicContentRef) item).getDynamicContent();
                writeTemplate(dynamicContent, object);
            }
            else {
                throw new OpenSearchHadoopIllegalArgumentException(String.format("Unknown object type received [%s][%s]", item, item.getClass()));
            }
        }
    }
}