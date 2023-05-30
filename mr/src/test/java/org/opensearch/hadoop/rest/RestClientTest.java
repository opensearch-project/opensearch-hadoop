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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.query.MatchAllQueryBuilder;
import org.opensearch.hadoop.rest.stats.Stats;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.FastByteArrayInputStream;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;

public class RestClientTest {

    @Test
    public void testPostTypelessDocumentSuccess() throws Exception {
        String index = "index";
        Settings settings = new TestSettings();
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index + "/_doc", null, document);
        String response =
                "{\n" +
                "  \"_index\": \"index\",\n" +
                "  \"_type\": \"_doc\",\n" +
                "  \"_id\": \"AbcDefGhiJklMnoPqrS_\",\n" +
                "  \"_version\": 1,\n" +
                "  \"result\": \"created\",\n" +
                "  \"_shards\": {\n" +
                "    \"total\": 2,\n" +
                "    \"successful\": 1,\n" +
                "    \"failed\": 0\n" +
                "  },\n" +
                "  \"_seq_no\": 0,\n" +
                "  \"_primary_term\": 1\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        String id = client.postDocument(writeResource, document);

        assertEquals("AbcDefGhiJklMnoPqrS_", id);
    }

    @Test(expected = OpenSearchHadoopInvalidRequest.class)
    public void testPostTypelessDocumentFailure() throws Exception {
        String index = "index";
        Settings settings = new TestSettings();
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index + "/_doc", null, document);
        String response =
                "{\n" +
                "  \"error\": {\n" +
                "    \"root_cause\": [\n" +
                "      {\n" +
                "        \"type\": \"io_exception\",\n" +
                "        \"reason\": \"test failure\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"type\": \"io_exception\",\n" +
                "    \"reason\": \"test failure\",\n" +
                "    \"caused_by\": {\n" +
                "      \"type\": \"io_exception\",\n" +
                "      \"reason\": \"This test needs to fail\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"status\": 400\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(400, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        client.postDocument(writeResource, document);

        fail("Request should have failed");
    }

    @Test(expected = OpenSearchHadoopInvalidRequest.class)
    public void testPostTypelessDocumentWeirdness() throws Exception {
        String index = "index";
        Settings settings = new TestSettings();
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        settings.setResourceWrite(index);
        Resource writeResource = new Resource(settings, false);
        BytesArray document = new BytesArray("{\"field\":\"value\"}");
        SimpleRequest request = new SimpleRequest(Request.Method.POST, null, index + "/_doc", null, document);
        String response =
                "{\n" +
                        "  \"_index\": \"index\",\n" +
                        "  \"_type\": \"_doc\",\n" +
                        "  \"definitely_not_an_id\": \"AbcDefGhiJklMnoPqrS_\",\n" + // Make the ID go away
                        "  \"_version\": 1,\n" +
                        "  \"result\": \"created\",\n" +
                        "  \"_shards\": {\n" +
                        "    \"total\": 2,\n" +
                        "    \"successful\": 1,\n" +
                        "    \"failed\": 0\n" +
                        "  },\n" +
                        "  \"_seq_no\": 0,\n" +
                        "  \"_primary_term\": 1\n" +
                        "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        String id = client.postDocument(writeResource, document);

        assertEquals("AbcDefGhiJklMnoPqrS_", id);
    }

    @Test
    public void testCount() throws Exception {
        String index = "index";
        String type = "type";

        BytesArray query = new BytesArray("{\"query\":{\"match_all\":{}}}");
        // the count API should always be typeless.
        SimpleRequest request = new SimpleRequest(Request.Method.GET, null, index + "/_search?size=0&track_total_hits=true", null, query);
        String response =
                "{\n" +
                        "    \"took\": 6,\n" +
                        "    \"timed_out\": false,\n" +
                        "    \"_shards\": {\n" +
                        "        \"total\": 1,\n" +
                        "        \"successful\": 1,\n" +
                        "        \"skipped\": 0,\n" +
                        "        \"failed\": 0\n" +
                        "    },\n" +
                        "    \"hits\": {\n" +
                        "        \"total\": {\n" +
                        "            \"value\": 5,\n" +
                        "            \"relation\": \"eq\"\n" +
                        "        },\n" +
                        "        \"max_score\": null,\n" +
                        "        \"hits\": []\n" +
                        "    }\n" +
                        "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        // Queue up two responses
        Mockito.when(mock.execute(Mockito.eq(request), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        Settings testSettings = new TestSettings();
        testSettings.setInternalVersion(OpenSearchMajorVersion.V_3_X);
        RestClient client = new RestClient(testSettings, mock);

        // Make sure that it works
        long count = client.count(index, MatchAllQueryBuilder.MATCH_ALL);
        assertEquals(5L, count);

        // Make sure that type is left off if it is included
        count = client.count(index, type, MatchAllQueryBuilder.MATCH_ALL);
        assertEquals(5L, count);
    }

    @Test(expected = OpenSearchHadoopParsingException.class)
    public void testCountBadRelation() throws Exception {
        String index = "index";

        BytesArray query = new BytesArray("{\"query\":{\"match_all\":{}}}");
        SimpleRequest request = new SimpleRequest(Request.Method.GET, null, index + "/_search?size=0&track_total_hits=true", null, query);
        String response =
                "{\n" +
                        "    \"took\": 6,\n" +
                        "    \"timed_out\": false,\n" +
                        "    \"_shards\": {\n" +
                        "        \"total\": 1,\n" +
                        "        \"successful\": 1,\n" +
                        "        \"skipped\": 0,\n" +
                        "        \"failed\": 0\n" +
                        "    },\n" +
                        "    \"hits\": {\n" +
                        "        \"total\": {\n" +
                        "            \"value\": 5,\n" +
                        "            \"relation\": \"gte\"\n" +
                        "        },\n" +
                        "        \"max_score\": null,\n" +
                        "        \"hits\": []\n" +
                        "    }\n" +
                        "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.eq(request), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        long count = client.count(index, MatchAllQueryBuilder.MATCH_ALL);

        assertEquals(5L, count);
    }

    @Test
    public void testMainInfo() {
        String response = "{\n" +
                "\"name\": \"node\",\n" +
                "\"cluster_name\": \"cluster\",\n" +
                "\"cluster_uuid\": \"uuid\",\n" +
                "\"version\": {\n" +
                "  \"number\": \"2.4.0\"\n" +
                "},\n" +
                "\"tagline\": \"The OpenSearch Project: https://opensearch.org/\"\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Map<String, List<String>> headers = new HashMap<>();
        Mockito.when(mock.execute(Mockito.any(SimpleRequest.class), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200", headers));

        RestClient client = new RestClient(new TestSettings(), mock);

        ClusterInfo clusterInfo = client.mainInfo();

        assertNotNull(clusterInfo.getClusterName());
        assertNotNull(clusterInfo.getClusterName().getUUID());
    }

    @Test
    public void testMainInfoWithClusterNotProvidingUUID() {
        String response = "{\n" +
                "\"name\": \"node\",\n" +
                "\"cluster_name\": \"cluster\",\n" +
                "\"version\": {\n" +
                "  \"number\": \"2.4.0\"\n" +
                "},\n" +
                "\"tagline\": \"The OpenSearch Project: https://opensearch.org/\"\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.any(SimpleRequest.class), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        ClusterInfo clusterInfo = client.mainInfo();

        assertNotNull(clusterInfo.getClusterName());
        assertNull(clusterInfo.getClusterName().getUUID());
    }

    @Test
    public void testMainInfoWithClusterProvidingUUID() {
        String response = "{\n" +
                "\"name\": \"node\",\n" +
                "\"cluster_name\": \"cluster\",\n" +
                "\"cluster_uuid\": \"uuid\",\n" +
                "\"version\": {\n" +
                "  \"number\": \"2.4.0\"\n" +
                "},\n" +
                "\"tagline\": \"The OpenSearch Project: https://opensearch.org/\"\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.any(SimpleRequest.class), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        ClusterInfo clusterInfo = client.mainInfo();

        assertNotNull(clusterInfo.getClusterName());
        assertEquals("uuid", clusterInfo.getClusterName().getUUID());
    }

    @Test
    public void testScroll() {
        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Stats stats = new Stats();
        Mockito.when(mock.transportStats()).thenReturn(stats);

        String response = "{}";
        // Note: scroll cannot use retries:
        Mockito.when(mock.execute(Mockito.any(SimpleRequest.class), Mockito.eq(false)))
                .thenReturn(new SimpleResponse(201, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        RestClient client = new RestClient(new TestSettings(), mock);

        InputStream result = client.scroll("_id");
        assertNotNull(result);

        Mockito.verify(mock).execute(Mockito.any(SimpleRequest.class), Mockito.eq(false));
    }

}