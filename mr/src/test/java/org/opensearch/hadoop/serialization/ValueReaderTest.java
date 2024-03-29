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

import java.io.InputStream;

import org.opensearch.hadoop.serialization.builder.JdkValueReader;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ValueReaderTest {

    private InputStream in;

    @Before
    public void before() throws Exception {
        in = getClass().getResourceAsStream("scroll-test.json");
    }

    @After
    public void after() throws Exception {
        in.close();
    }

    @Test
    public void testSimplePathReader() throws Exception {
        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(new JdkValueReader(), new TestSettings()).setReadMetadata(false));
        reader.read(in);
    }

    @Test
    public void testSimplePathReaderJson() throws Exception {
        ScrollReader reader = new ScrollReader(ScrollReaderConfigBuilder.builder(new JdkValueReader(), new TestSettings()).setReadMetadata(false).setReturnRawJson(true));
        reader.read(in);
    }
}