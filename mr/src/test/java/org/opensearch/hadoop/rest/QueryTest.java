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

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryTest {
    private TestSettings cfg;
    private SearchRequestBuilder builder;

    @Before
    public void setup() {
        cfg = new TestSettings();
        builder = new SearchRequestBuilder(true);
    }

    @Test
    public void testSimpleQueryTypeless() {
        cfg.setInternalVersion(OpenSearchMajorVersion.LATEST);
        cfg.setResourceRead("foo");
        Resource typeless = new Resource(cfg, true);
        assertTrue(builder.resource(typeless).toString().contains("foo"));
        assertTrue(builder.indices("foo").toString().contains("foo"));
    }

    @Test
    public void testExcludeSourceTrue() {
        assertTrue(builder.excludeSource(true).toString().contains("\"_source\":false"));
    }

    @Test
    public void testExcludeSourceFalse() {
        assertTrue(builder.fields("a,b").excludeSource(false).toString().contains("\"_source\":[\"a\",\"b\"]"));
    }

    @Test
    public void testEmptySource() {
        assertFalse(builder.fields("").excludeSource(false).toString().contains("\"_source\""));
    }

    @Test(expected= OpenSearchHadoopIllegalArgumentException.class)
    public void testExcludeSourceAndGetFields() {
        builder.fields("a,b").excludeSource(true);
    }

    @Test(expected= OpenSearchHadoopIllegalArgumentException.class)
    public void testGetFieldsAndExcludeSource() {
        builder.excludeSource(true).fields("a,b");
    }
}