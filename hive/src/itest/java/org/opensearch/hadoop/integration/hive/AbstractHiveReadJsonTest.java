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

package org.opensearch.hadoop.integration.hive;

import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.QueryTestParams;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.hadoop.util.TestUtils.resource;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
@RunWith(Parameterized.class)
public class AbstractHiveReadJsonTest {

    private static int testInstance = 0;
    private final boolean readMetadata;
    private OpenSearchMajorVersion targetVersion;

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @Parameters
    public static Collection<Object[]> queries() {
        return new QueryTestParams(tempFolder).params();
    }

    private final String query;

    public AbstractHiveReadJsonTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        HiveSuite.provisionOpenSearchLib();
        RestUtils.refresh("json-hive*");
        targetVersion = TestUtils.getOpenSearchClusterInfo().getMajorVersion();
        new QueryTestParams(tempFolder).provisionQueries(HdpBootstrap.hadoopConfig());
    }

    @After
    public void after() throws Exception {
        testInstance++;
        HiveSuite.after();
    }

    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (data INT, garbage INT, garbage2 STRING) "
                + tableProps(resource("json-hive-artists", "data", targetVersion), "'opensearch.output.json' = 'true'", "'opensearch.mapping.names'='garbage2:refuse'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        HiveSuite.server.execute(create);
        List<String> result = HiveSuite.server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test
    public void basicLoadWithNameMappings() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (refuse INT, garbage INT, data STRING) "
                + tableProps(resource("json-hive-artists", "data", targetVersion), "'opensearch.output.json' = 'true'", "'opensearch.mapping.names'='data:boomSomethingYouWerentExpecting'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        HiveSuite.server.execute(create);
        List<String> result = HiveSuite.server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test(expected = SQLException.class)
    public void basicLoadWithNoGoodCandidateField() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (refuse INT, garbage INT) "
                + tableProps(resource("json-hive-artists", "data", targetVersion), "'opensearch.output.json' = 'true'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        HiveSuite.server.execute(create);
        HiveSuite.server.execute(select);

        fail("Should have broken because there are no String fields in the table schema to place the JSON data.");
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE jsonmissingread" + testInstance + " (data STRING) "
                + tableProps(resource("foobar", "missing", targetVersion), "'opensearch.index.read.missing.as.empty' = 'true'", "'opensearch.output.json' = 'true'");

        String select = "SELECT * FROM jsonmissingread" + testInstance;

        HiveSuite.server.execute(create);
        List<String> result = HiveSuite.server.execute(select);
        assertEquals(0, result.size());
    }

    @Test
    public void testNoSourceFilterCollisions() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistscollisionread" + testInstance + " (data INT, garbage INT, garbage2 STRING) "
                + tableProps(
                    resource("json-hive-artists", "data", targetVersion),
                    "'opensearch.output.json' = 'true'",
                    "'opensearch.read.source.filter'='name'"
                );

        String select = "SELECT * FROM jsonartistscollisionread" + testInstance;

        HiveSuite.server.execute(create);
        List<String> result = HiveSuite.server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        System.out.println(result);
        assertContains(result, "Marilyn");
        assertThat(result, not(hasItem(containsString("last.fm/music/MALICE"))));
        assertThat(result, not(hasItem(containsString("last.fm/serve/252/5872875.jpg"))));
    }

    private static boolean containsNoNull(List<String> str) {
        for (String string : str) {
            if (string.contains("NULL")) {
                return false;
            }
        }

        return true;
    }

    private static void assertContains(List<String> str, String content) {
        for (String string : str) {
            if (string.contains(content)) {
                return;
            }
        }
        fail(String.format("'%s' not found in %s", content, str));
    }


    private String tableProps(String resource, String... params) {
        List<String> copy = new ArrayList(Arrays.asList(params));
        copy.add("'" + ConfigurationOptions.OPENSEARCH_READ_METADATA + "'='" + readMetadata + "'");
        return HiveSuite.tableProps(resource, query, copy.toArray(new String[copy.size()]));
    }
}