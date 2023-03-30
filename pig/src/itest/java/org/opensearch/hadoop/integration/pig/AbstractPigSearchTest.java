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
package org.opensearch.hadoop.integration.pig;

import java.nio.file.Paths;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;
import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.QueryTestParams;
import org.opensearch.hadoop.OpenSearchAssume;
import org.opensearch.hadoop.mr.HadoopCfgUtils;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.opensearch.hadoop.util.TestUtils.resource;
import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;


@RunWith(Parameterized.class)
public class AbstractPigSearchTest extends AbstractPigTests {

    private static int testInstance = 0;
    private static String previousQuery;
    private static Configuration testConfiguration = HdpBootstrap.hadoopConfig();
    private static String workingDir = HadoopCfgUtils.isLocal(testConfiguration) ? Paths.get("").toAbsolutePath().toString() : "/";

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @Parameters
    public static Collection<Object[]> queries() {
        return new QueryTestParams(tempFolder).params();
    }

    private final String query;
    private final boolean readMetadata;
    private final OpenSearchMajorVersion VERSION = TestUtils.getOpenSearchClusterInfo().getMajorVersion();

    public AbstractPigSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;

        if (!query.equals(previousQuery)) {
            previousQuery = query;
            testInstance++;
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        // we do this just here since the configuration doesn't get used in Pig scripts.
        new QueryTestParams(tempFolder).provisionQueries(AbstractPigTests.testConfiguration);
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh("pig*");
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');" +
                "A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' USING OpenSearchStorage();" +
                "X = LIMIT A 3;" +
                //"DESCRIBE A;";
                "STORE A INTO '" + tmpPig() + "/testtuple';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testtuple");
        assertThat(results, containsString("Behemoth"));
        assertThat(results, containsString("Megadeth"));
    }

    @Test
    public void testTupleCount() throws Exception {
        String script = "A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' using org.opensearch.hadoop.pig.OpenSearchStorage();" +
                "COUNT = FOREACH (GROUP A ALL) GENERATE COUNT(A);" +
                "DUMP COUNT;";

        pig.executeScript(script);
    }

    @Test
    public void testTupleWithSchema() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');" +
                "A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' USING OpenSearchStorage() AS (name:chararray);" +
                "X = LIMIT A 3;" +
                "STORE A INTO '" + tmpPig() + "/testtupleschema';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtupleschema");
        assertThat(results, containsString("Behemoth"));
        assertThat(results, containsString("Megadeth"));
    }

    @Test
    @Ignore("This seems to break on Hadoop 3 due to some sort of Pig plan serialization bug")
    public void testBagWithSchema() throws Exception {
        String script =
                      "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query + "', 'opensearch.mapping.names=data:name','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');" +
                      "A = LOAD '"+resource("pig-bagartists", "data", VERSION)+"' USING OpenSearchStorage() AS (data: chararray);"
                      + "B = ORDER A BY * DESC;"
                      + "X = LIMIT B 3;"
                      + "STORE X INTO '" + tmpPig() + "/testbagschema';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testbagschema");
        assertThat(results, containsString("xotox"));
        assertThat(results, containsString("t.A.T.u"));
        assertThat(results, containsString("strom noir"));
    }

    @Test
    public void testTimestamp() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query
                        + "','opensearch.read.metadata=" + readMetadata +
                      "','opensearch.output.json=true');" +
                      "A = LOAD '"+resource("pig-timestamp", "data", VERSION)+"' USING OpenSearchStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testtimestamp';";
        pig.executeScript(script);
        System.out.println(getResults("" + tmpPig() + "/testtimestamp"));
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                      "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage(" +
                      "'opensearch.mapping.names=nAme:name, timestamp:@timestamp, uRL:url, picturE:picture', 'opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');"
                        +"A = LOAD '"+resource("pig-fieldalias", "data", VERSION)+"' USING OpenSearchStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testfieldlalias';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testfieldlalias");

        assertThat(results, containsString("Megadeth"));
        assertThat(results, containsString("http://www.last.fm/music/Megadeth"));
        assertThat(results, containsString("Blur"));
        assertThat(results, containsString("http://www.last.fm/music/Gorillaz"));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script =
                      "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.index.read.missing.as.empty=true','opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');"
                        + "A = LOAD '"+resource("foo", "bar", VERSION)+"' USING OpenSearchStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE X INTO '" + tmpPig() + "/testmissingindex';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testmissingindex");
        assertThat(results.length(), is(0));
    }

    @Test
    public void testNestedObject() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata 
                        + "','opensearch.output.json=true');"
                        +"A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' USING OpenSearchStorage() AS (name: chararray, links: tuple(chararray));"
                + "B = FOREACH A GENERATE name, links;"
                + "C = ORDER B BY name DESC;"
                + "D = LIMIT C 3;"
                + "STORE C INTO '" + tmpPig() + "/testnestedobject';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testnestedobject");

        assertThat(results, containsString("Megadeth"));
        assertThat(results, containsString("http://www.last.fm/music/Megadeth"));
        assertThat(results, containsString("Blur"));
        assertThat(results, containsString("http://www.last.fm/music/Gorillaz"));
    }

    @Test
    public void testSourceFilterCollisionNoSchema() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query
                        + "','opensearch.read.metadata=" + readMetadata +
                        "','opensearch.read.source.filter=name','opensearch.output.json=true');" +
                        "A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' USING OpenSearchStorage();" +
                        "X = LIMIT A 3;" +
                        "DUMP X;" +
                        "STORE A INTO '" + tmpPig() + "/nocollision';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/nocollision");
        assertThat(results, containsString("Behemoth"));
        assertThat(results, containsString("Megadeth"));
        assertThat(results, containsString("Foo Fighters"));
    }

    @Test(expected = OpenSearchHadoopIllegalStateException.class)
    public void testSourceFilterCollisionWithSchemaAndProjectionPushdown() throws Exception {
        String script =
                        "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.query=" + query + "','opensearch.read.metadata=" + readMetadata +"','opensearch.read.source.filter=name');" +
                        "A = LOAD '"+resource("pig-tupleartists", "data", VERSION)+"' USING OpenSearchStorage() AS (name: chararray, links: chararray);" +
                        "B = FOREACH A GENERATE name;" +
                        "X = LIMIT B 3;" +
                        //"DESCRIBE A;";
                        "STORE B INTO '" + tmpPig() + "/collision';";
        pig.executeScript(script);
        fail("Should not have made it to here: User specified source filtering should have broken when connector accepts projection pushdown from Pig because the 'links' field is unused in later steps.");
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-1", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-5", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-9", "data", VERSION)));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-format-2001-10-06", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-format-2005-10-06", "data", VERSION)));
        Assert.assertTrue(RestUtils.exists(resource("pig-pattern-format-2017-10-06", "data", VERSION)));
    }

    @Test
    public void testNestedTuple() throws Exception {
        String script =
                "DEFINE OpenSearchStorage org.opensearch.hadoop.pig.OpenSearchStorage('opensearch.output.json=true');"
                + "A = LOAD '"+resource("pig-nestedtuple", "data", VERSION)+"' USING OpenSearchStorage();"
                //+ "B = FOREACH A GENERATE COUNT(my_array) AS count;"
                //+ "ILLUSTRATE B;"
                + "X = LIMIT A 3;"
                + "STORE A INTO '" + tmpPig() + "/testnestedtuple';";
        pig.executeScript(script);
        String results = getResults("" + tmpPig() + "/testnestedtuple");
        assertThat(results, containsString("{\"my_array\" : [\"1.a\",\"1.b\"]}"));
        assertThat(results, containsString("{\"my_array\" : [\"2.a\",\"2.b\"]}"));
    }

    private static String tmpPig() {
        return new Path("tmp-pig/search-" + testInstance)
                .makeQualified(FileSystem.getDefaultUri(AbstractPigTests.testConfiguration), new Path(workingDir))
                .toUri()
                .toString();
    }
}