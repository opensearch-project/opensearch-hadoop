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

import org.opensearch.hadoop.rest.query.BoolQueryBuilder;
import org.opensearch.hadoop.rest.query.ConstantScoreQueryBuilder;
import org.opensearch.hadoop.rest.query.QueryBuilder;
import org.opensearch.hadoop.rest.query.TermQueryBuilder;
import org.opensearch.hadoop.rest.request.GetAliasesRequestBuilder;
import org.opensearch.hadoop.serialization.dto.IndicesAliases;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.junit.Assert;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.JsonParser;
import org.opensearch.hadoop.thirdparty.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class ApplyAliasMetaDataTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    private static final OpenSearchMajorVersion[] ES_VERSIONS =
            new OpenSearchMajorVersion[]{
                    OpenSearchMajorVersion.V_0_X,
                    OpenSearchMajorVersion.V_1_X,
                    OpenSearchMajorVersion.V_2_X,
                    OpenSearchMajorVersion.V_5_X
            };

    @Test
    public void testNoAlias() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-empty-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");
            SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
            RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1");
            assertNull(searchRequest.query());
            assertNull(searchRequest.routing());
        }
    }

    @Test
    public void testNoExplicitAlias() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "index1");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "_all");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "*");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "a*1", "index*");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1", "+index1");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1", "alias2", "*");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1", "alias2", "_all");
                assertNull(searchRequest.query());
                assertNull(searchRequest.routing());
            }
        }
    }

    @Test
    public void testOneAlias() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");
            QueryBuilder expected = new TermQueryBuilder().field("system").term("hadoop");

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1");
                Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                        QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
                assertEquals("1,2", searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "a*1");
                Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                        QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
                assertEquals("1,2", searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias*", "-alias2");
                Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                        QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
                assertEquals("1,2", searchRequest.routing());
            }

            {
                SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
                RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "+alias1", "+alias2", "-alias2");
                Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                        QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
                assertEquals("1,2", searchRequest.routing());
            }
        }
    }

    @Test
    public void testOneAliasWithQuery() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");
            SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
            QueryBuilder query = new TermQueryBuilder().field("user").term("costin");
            searchRequest.query(query);
            RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1");
            QueryBuilder expected;
            if (version.after(OpenSearchMajorVersion.V_1_X)) {
                expected = new BoolQueryBuilder()
                        .must(
                                new TermQueryBuilder().field("user").term("costin")
                        )
                        .filter(
                                new TermQueryBuilder().field("system").term("hadoop")
                        );
            } else {
                expected = new BoolQueryBuilder()
                        .must(
                                new TermQueryBuilder().field("user").term("costin")
                        )
                        .must(
                                new ConstantScoreQueryBuilder()
                                        .filter(
                                                new TermQueryBuilder().field("system").term("hadoop")
                                        )
                                        .boost(0.0f)
                        );
            }
            Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                    QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
            assertEquals("1,2", searchRequest.routing());
        }
    }

    @Test
    public void testTwoAliases() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");
            SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
            RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1", "alias2");
            QueryBuilder expected = new BoolQueryBuilder()
                    .should(
                            new TermQueryBuilder()
                                    .field("system")
                                    .term("hadoop")
                    )
                    .should(
                            new TermQueryBuilder()
                                    .field("system")
                                    .term("spark")
                    );
            Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                    QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
            assertEquals("1,2", searchRequest.routing());
        }
    }

    @Test
    public void testTwoAliasesWithQuery() throws IOException {
        for (OpenSearchMajorVersion version : ES_VERSIONS) {
            Map<String, Object> map = MAPPER.readValue(getClass().getResourceAsStream("get-aliases-response.json"), TreeMap.class);
            GetAliasesRequestBuilder.Response response = new GetAliasesRequestBuilder.Response(map);
            Map<String, IndicesAliases.Alias> aliases = response.getIndices().getAliases("index1");
            SearchRequestBuilder searchRequest = new SearchRequestBuilder(version, false);
            QueryBuilder query = new TermQueryBuilder().field("user").term("costin");
            searchRequest.query(query);
            RestService.applyAliasMetadata(version, aliases, searchRequest, "index1", "alias1", "alias2");
            QueryBuilder expected;
            if (version.after(OpenSearchMajorVersion.V_1_X)) {
                expected = new BoolQueryBuilder()
                        .must(
                                new TermQueryBuilder().field("user").term("costin")
                        )
                        .filter(
                                new BoolQueryBuilder()
                                        .should(
                                                new TermQueryBuilder().field("system").term("hadoop")
                                        )
                                        .should(
                                                new TermQueryBuilder().field("system").term("spark")
                                        )
                        );
            } else {
                expected = new BoolQueryBuilder()
                        .must(
                                new TermQueryBuilder().field("user").term("costin")
                        )
                        .must(
                                new ConstantScoreQueryBuilder()
                                        .filter(
                                                new BoolQueryBuilder()
                                                        .should(
                                                                new TermQueryBuilder().field("system").term("hadoop")
                                                        )
                                                        .should(
                                                                new TermQueryBuilder().field("system").term("spark")
                                                        )
                                        )
                                        .boost(0.0f)
                        );
            }
            Assert.assertEquals(QueryBuilderTestUtils.printQueryBuilder(expected, false),
                    QueryBuilderTestUtils.printQueryBuilder(searchRequest.query(), false));
            assertEquals("1,2", searchRequest.routing());
        }
    }
}