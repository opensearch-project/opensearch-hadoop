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
package org.opensearch.hadoop.integration.mr;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.QueryTestParams;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.OpenSearchAssume;
import org.opensearch.hadoop.mr.EsInputFormat;
import org.opensearch.hadoop.mr.HadoopCfgUtils;
import org.opensearch.hadoop.mr.LinkedMapWritable;
import org.opensearch.hadoop.mr.PrintStreamOutputFormat;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.opensearch.hadoop.util.TestUtils.docEndpoint;
import static org.opensearch.hadoop.util.TestUtils.resource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class AbstractMROldApiSearchTest {

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @Parameters
    public static Collection<Object[]> queries() {
        return new QueryTestParams(tempFolder).jsonParams();
    }

    private final String query;
    private final String indexPrefix;
    private final Random random = new Random();
    private final boolean readMetadata;
    private final boolean readAsJson;
    private final ClusterInfo clusterInfo;

    public AbstractMROldApiSearchTest(String indexPrefix, String query, boolean readMetadata, boolean readAsJson) {
        this.query = query;
        this.indexPrefix = indexPrefix;
        this.readMetadata = readMetadata;
        this.readAsJson = readAsJson;
        this.clusterInfo = TestUtils.getOpenSearchClusterInfo();
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "mroldapi*");
    }

    @Test
    public void testBasicReadWithConstantRouting() throws Exception {
        String type = "data";
        String target = resource(indexPrefix + "mroldapi-savewithconstantrouting", type, clusterInfo.getMajorVersion());

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ROUTING, "<foobar/>");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, target);

        JobClient.runJob(conf);
    }

    @Test
    public void testBasicSearch() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mroldapi-save", "data", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }


    @Test
    public void testBasicSearchWithWildCard() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrold*-save", "data", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mroldapi-savewithid", "data", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("foobar", "save", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchCreated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mroldapi-update", "data", clusterInfo.getMajorVersion()));

        JobClient.runJob(conf);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-1", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-5", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-9", "data", clusterInfo.getMajorVersion())));
    }

    @Test
    public void testDynamicPatternWithFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-format-2001-10-06", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-format-2005-10-06", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mroldapi-pattern-format-2017-10-06", "data", clusterInfo.getMajorVersion())));
    }

    @Test
    public void testUpsertOnlyParamScriptWithArrayOnArrayField() throws Exception {
        String target = docEndpoint("mroldapi-createwitharrayupsert", "data", clusterInfo.getMajorVersion()) + "/1";
        Assert.assertTrue(RestUtils.exists(target));
        String result = RestUtils.get(target);
        assertThat(result, not(containsString("ArrayWritable@")));
    }

    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mroldapi-nested", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        //conf.set(Stream.class.getName(), "OUT");
        JobClient.runJob(conf);
    }

    private JobConf createJobConf() throws IOException {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(EsInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);
        conf.setOutputValueClass(mapType);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.set(ConfigurationOptions.OPENSEARCH_QUERY, query);
        conf.setNumReduceTasks(0);

        conf.set(ConfigurationOptions.ES_READ_METADATA, String.valueOf(readMetadata));
        conf.set(ConfigurationOptions.ES_READ_METADATA_VERSION, String.valueOf(true));
        conf.set(ConfigurationOptions.OPENSEARCH_OUTPUT_JSON, String.valueOf(readAsJson));

        new QueryTestParams(tempFolder).provisionQueries(conf);
        FileInputFormat.setInputPaths(conf, new Path(MRSuite.testData.sampleArtistsDatUri()));

        HdpBootstrap.addProperties(conf, TestSettings.TESTING_PROPS, false);
        return conf;
    }
}