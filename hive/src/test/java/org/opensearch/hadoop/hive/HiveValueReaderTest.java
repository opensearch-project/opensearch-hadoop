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
package org.opensearch.hadoop.hive;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.opensearch.hadoop.serialization.ScrollReader;
import org.opensearch.hadoop.serialization.ScrollReaderConfigBuilder;
import org.opensearch.hadoop.serialization.dto.mapping.FieldParser;
import org.opensearch.hadoop.serialization.dto.mapping.Mapping;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HiveValueReaderTest {

    @Test
    public void testDateMapping() throws Exception {
        ScrollReaderConfigBuilder scrollCfg = ScrollReaderConfigBuilder.builder(new HiveValueReader(), new TestSettings())
                .setResolvedMapping(mapping("hive-date-mappingresponse.json"))
                .setReadMetadata(false)
                .setReturnRawJson(false)
                .setIgnoreUnmappedFields(false);
        ScrollReader reader = new ScrollReader(scrollCfg);
        InputStream stream = getClass().getResourceAsStream("hive-date-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] doc = read.get(0);
        Map map = (Map) doc[1];
        assertTrue(map.containsKey(new Text("type")));
        assertTrue(map.containsKey(new Text("&t")));
        assertThat(map.get(new Text("&t")).toString(), containsString("2014-08-05"));
    }

    private Mapping mapping(String resource) throws Exception {
        InputStream stream = getClass().getResourceAsStream(resource);
        return FieldParser.parseTypelessMappings(new ObjectMapper().readValue(stream, Map.class)).getResolvedView();
    }
}