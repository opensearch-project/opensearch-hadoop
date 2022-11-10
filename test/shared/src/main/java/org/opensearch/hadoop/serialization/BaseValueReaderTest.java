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
package org.opensearch.hadoop.serialization;

import org.opensearch.hadoop.serialization.builder.JdkValueReader;
import org.opensearch.hadoop.serialization.builder.ValueReader;
import org.opensearch.hadoop.util.DateUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.hamcrest.CoreMatchers.nullValue;


public class BaseValueReaderTest extends AbstractValueReaderTest {

    @Override
    public ValueReader createValueReader() {
        return new JdkValueReader();
    }

    @Override
    public void checkNull(Object typeFromJson) {
        assertThat(typeFromJson, nullValue());
    }

    @Override
    public void checkEmptyString(Object typeFromJson) {
        assertEquals(null, typeFromJson);
    }

    @Override
    public void checkString(Object typeFromJson) {
        assertEquals("someText", typeFromJson);
    }

    @Override
    public void checkInteger(Object typeFromJson) {
        assertEquals(Integer.MAX_VALUE, typeFromJson);
    }

    @Override
    public void checkLong(Object typeFromJson) {
        assertEquals(Long.MAX_VALUE, typeFromJson);
    }

    @Override
    public void checkDouble(Object typeFromJson) {
        assertEquals(Double.MAX_VALUE, typeFromJson);
    }

    @Override
    public void checkFloat(Object typeFromJson) {
        assertEquals(Float.MAX_VALUE + "", typeFromJson + "");
    }

    @Override
    public void checkBoolean(Object typeFromJson) {
        assertEquals(Boolean.TRUE, typeFromJson);
    }

    @Override
    public void checkByteArray(Object typeFromJson, String encode) {
        assertEquals(encode, typeFromJson);
    }

    @Override
    public void checkBinary(Object typeFromJson, byte[] encode) { assertArrayEquals(encode, (byte[])typeFromJson); }

    @Test
    public void parseJDKRichDateISO() {
        Object withColon = DateUtils.parseDate("2015-05-25T22:30:00+03:00");
        Object withoutColon = DateUtils.parseDate("2015-05-25T22:30:00+0300");
        Object noMinutes = DateUtils.parseDate("2015-05-25T22:30:00+03");

        assertEquals(withColon, withoutColon);
        assertEquals(withColon, noMinutes);
    }
}