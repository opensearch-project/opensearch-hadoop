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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.QueryTestParams;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.mr.OpenSearchInputFormat;
import org.opensearch.hadoop.mr.HadoopCfgUtils;
import org.opensearch.hadoop.mr.LinkedMapWritable;
import org.opensearch.hadoop.mr.PrintStreamOutputFormat;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.ClusterInfo;
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

import static org.opensearch.hadoop.util.TestUtils.resource;

@RunWith(Parameterized.class)
public class AbstractMRNewApiSearchTest {

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

    public AbstractMRNewApiSearchTest(String indexPrefix, String query, boolean readMetadata, boolean readAsJson) {
        this.indexPrefix = indexPrefix;
        this.query = query;
        this.readMetadata = readMetadata;
        this.readAsJson = readAsJson;
        this.clusterInfo = TestUtils.getOpenSearchClusterInfo();
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "mrnewapi*");
    }

    @Test
    public void testBasicSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrnewapi-save", "data", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testBasicWildSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrnew*-save", "data", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrnewapi-savewithid", "data", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        Configuration conf = createConf();
        conf.setBoolean(ConfigurationOptions.OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "foobar", "save", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchCreated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrnewapi-createwithid", "data", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource(indexPrefix + "mrnewapi-update", "data", clusterInfo.getMajorVersion()));

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-1", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-5", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-9", "data", clusterInfo.getMajorVersion())));
    }

    @Test
    public void testDynamicPatternWithFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-format-2001-10-06", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-format-2005-10-06", "data", clusterInfo.getMajorVersion())));
        Assert.assertTrue(RestUtils.exists(resource("mrnewapi-pattern-format-2017-10-06", "data", clusterInfo.getMajorVersion())));
    }

    private Configuration createConf() throws IOException {
        Configuration conf = HdpBootstrap.hadoopConfig();
        HadoopCfgUtils.setGenericOptions(conf);
        Job job = new Job(conf);
        job.setInputFormatClass(OpenSearchInputFormat.class);
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);

        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);

        job.setOutputValueClass(mapType);
        conf.set(ConfigurationOptions.OPENSEARCH_QUERY, query);

        conf.set(ConfigurationOptions.OPENSEARCH_READ_METADATA, String.valueOf(readMetadata));
        conf.set(ConfigurationOptions.OPENSEARCH_OUTPUT_JSON, String.valueOf(readAsJson));

        new QueryTestParams(tempFolder).provisionQueries(conf);
        job.setNumReduceTasks(0);
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);

        Configuration cfg = job.getConfiguration();
        HdpBootstrap.addProperties(cfg, TestSettings.TESTING_PROPS, false);
        return cfg;
    }
}