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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.encoding.HttpEncodingTools;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ResourceTest {

    @Parameters
    public static Collection<Object[]> params() {
        List<Object[]> parameters = new ArrayList<Object[]>();
        parameters.add(new Object[]{OpenSearchMajorVersion.LATEST, true});
        parameters.add(new Object[]{OpenSearchMajorVersion.LATEST, false});

        parameters.add(new Object[]{OpenSearchMajorVersion.V_1_X, true});
        parameters.add(new Object[]{OpenSearchMajorVersion.V_1_X, false});

        return parameters;
    }

    private final OpenSearchMajorVersion testVersion;
    private final boolean readResource;

    public ResourceTest(OpenSearchMajorVersion testVersion, boolean readResource) {
        this.testVersion = testVersion;
        this.readResource = readResource;
    }

    @Test
    public void testJustIndex() throws Exception {
        Resource res = createResource("foo");
        assertEquals("foo", res.toString());
    }

    @Test
    public void testAll() throws Exception {
        Resource res = createResource("_all");
        assertEquals("_all", res.toString());
    }

    @Test
    public void testUnderscore() throws Exception {
        Resource res = createResource("fo_o");
        assertEquals("fo_o", res.toString());
    }

    @Test
    public void testQueryUri() throws Exception {
        Settings s = new TestSettings();
        Resource res = createResource("foo/_search=?somequery", s);
        assertEquals("foo", res.toString());
        assertEquals("?somequery", s.getQuery());
    }

    @Test
    public void testQueryUriWithParams() throws Exception {
        Settings s = new TestSettings();
        Resource res = createResource("foo/_search=?somequery&bla=bla", s);
        assertEquals("foo", res.toString());
        assertEquals("?somequery&bla=bla", s.getQuery());
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testQueryUriConflict() throws Exception {
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.OPENSEARCH_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/_search=?somequery", s);
        assertEquals("foo", res.toString());
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testQueryUriConflictWithParams() throws Exception {
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.OPENSEARCH_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/_search=?somequery&bla=bla", s);
        assertEquals("foo", res.toString());
    }

    @Test
    public void testDynamicFieldLowercase() throws Exception {
        Resource res = createResource("foo");
        res = createResource("foo-{F}");
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testNoWhitespaceAllowed() throws Exception {
        createResource("foo, bar/far");
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testNoWhitespaceAllowedTypeless() throws Exception {
        createResource("foo, bar");
    }

    @Test
    public void testBulkWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, "ingest-pipeline");
        Resource res = createResource("pipeline", settings);
        assertEquals("pipeline", res.toString());
        assertEquals("pipeline/_aliases", res.aliases());
        assertEquals("pipeline/_bulk?pipeline=ingest-pipeline", res.bulk());
        assertEquals("pipeline/_refresh", res.refresh());
    }


    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testBulkWithBadIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, "ingest pipeline");
        createResource("pipeline/test", settings);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testBulkUpdateBreaksWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, ConfigurationOptions.OPENSEARCH_OPERATION_UPDATE);
        createResource("pipeline/test", settings);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testBulkUpsertBreaksWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, ConfigurationOptions.OPENSEARCH_OPERATION_UPSERT);
        createResource("pipeline/test", settings);
    }

    private Resource createResource(String target) {
        return createResource(target, new TestSettings());
    }

    private Resource createResource(String target, Settings s) {
        s.setInternalVersion(testVersion);
        s.setProperty(ConfigurationOptions.OPENSEARCH_RESOURCE, target);
        return new Resource(s, true);
    }

    @Test
    public void testURiEscaping() throws Exception {
        assertEquals("http://localhost:9200/index/type%7Cfoo?q=foo%7Cbar:bar%7Cfoo", HttpEncodingTools.encodeUri("http://localhost:9200/index/type|foo?q=foo|bar:bar|foo"));
        assertEquals("foo%7Cbar", HttpEncodingTools.encodeUri("foo|bar"));
    }
}