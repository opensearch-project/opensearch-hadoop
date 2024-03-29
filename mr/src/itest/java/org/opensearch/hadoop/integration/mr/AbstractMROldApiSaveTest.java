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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.Stream;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.mr.OpenSearchOutputFormat;
import org.opensearch.hadoop.mr.HadoopCfgUtils;
import org.opensearch.hadoop.mr.LinkedMapWritable;
import org.opensearch.hadoop.mr.MultiOutputFormat;
import org.opensearch.hadoop.mr.PrintStreamOutputFormat;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.StringUtils;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.TestUtils;
import org.opensearch.hadoop.util.WritableUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.opensearch.hadoop.util.TestUtils.docEndpoint;
import static org.opensearch.hadoop.util.TestUtils.resource;
import static org.junit.Assume.assumeFalse;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractMROldApiSaveTest {

    private ClusterInfo clusterInfo = TestUtils.getOpenSearchClusterInfo();

    public static class TabMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, Object> entry = new LinkedHashMap<String, Object>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            entry.put("list", Arrays.asList("quick", "brown", "fox"));

            while (st.hasMoreTokens()) {
                String str = st.nextToken();
                if (str.startsWith("http")) {
                    entry.put("picture", str);
                } else if (str.startsWith("20")) {
                    entry.put("@timestamp", str);
                } else if (str.startsWith("1") || str.startsWith("2") || str.startsWith("5") || str.startsWith("9") || str.startsWith("10")) {
                    entry.put("tag", str);
                }
            }

            output.collect(key, WritableUtils.toWritable(entry));
        }
    }

    public static class ConstantMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            MapWritable map = new MapWritable();
            map.put(new Text("key"), new Text("value"));
            output.collect(new LongWritable(), map);
        }
    }


    public static class SplittableTextInputFormat extends TextInputFormat {

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            return super.getSplits(job, job.getInt("actual.splits", 3));
        }
    }

    @Parameters
    public static Collection<Object[]> configs() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(SplittableTextInputFormat.class);
        conf.setOutputFormat(OpenSearchOutputFormat.class);
        conf.setReducerClass(IdentityReducer.class);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.setNumMapTasks(2);
        conf.setInt("actual.splits", 2);
        conf.setNumReduceTasks(0);


        JobConf standard = new JobConf(conf);
        standard.setMapperClass(TabMapper.class);
        standard.setMapOutputValueClass(LinkedMapWritable.class);
        standard.set(ConfigurationOptions.OPENSEARCH_INPUT_JSON, "false");
        FileInputFormat.setInputPaths(standard, new Path(MRSuite.testData.sampleArtistsDat(conf)));

        JobConf json = new JobConf(conf);
        json.setMapperClass(IdentityMapper.class);
        json.setMapOutputValueClass(Text.class);
        json.set(ConfigurationOptions.OPENSEARCH_INPUT_JSON, "true");
        FileInputFormat.setInputPaths(json, new Path(MRSuite.testData.sampleArtistsJson(conf)));

        return Arrays.asList(new Object[][] {
                { standard, "" },
                { json, "json-" }
        });
    }

    private String indexPrefix = "";
    private final JobConf config;

    public AbstractMROldApiSaveTest(JobConf config, String indexPrefix) {
        this.indexPrefix = indexPrefix;
        this.config = config;

        HdpBootstrap.addProperties(config, TestSettings.TESTING_PROPS, false);
    }

    @Test
    public void testBasicMultiSave() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("oldapi-multi-save", "data", clusterInfo.getMajorVersion()));

        MultiOutputFormat.addOutputFormat(conf, OpenSearchOutputFormat.class);
        MultiOutputFormat.addOutputFormat(conf, PrintStreamOutputFormat.class);
        //MultiOutputFormat.addOutputFormat(conf, TextOutputFormat.class);

        PrintStreamOutputFormat.stream(conf, Stream.OUT);
        //conf.set("mapred.output.dir", "foo/bar");
        //FileOutputFormat.setOutputPath(conf, new Path("foo/bar"));

        conf.setClass("mapred.output.format.class", MultiOutputFormat.class, OutputFormat.class);
        runJob(conf);
    }


    @Test
    public void testNoInput() throws Exception {
        JobConf conf = createJobConf();

        // use only when dealing with constant input
        assumeFalse(conf.get(ConfigurationOptions.OPENSEARCH_INPUT_JSON).equals("true"));
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-constant", "data", clusterInfo.getMajorVersion()));
        conf.setMapperClass(ConstantMapper.class);

        runJob(conf);
    }

    @Test
    public void testBasicIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-save", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-savewithid", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithExtractedRouting() throws Exception {
        String index = indexPrefix + "mroldapi-savewithdynamicrouting";
        String type = "data";
        String target = resource(index, type, clusterInfo.getMajorVersion());

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ROUTING, "number");

        RestUtils.touch(index);
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, index);
        RestUtils.putMapping(index, type, StringUtils.toUTF("{\"_routing\": {\"required\":true}}"));

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithConstantRouting() throws Exception {
        String index = indexPrefix + "mroldapi-savewithconstantrouting";
        String type = "data";
        String target = resource(index, type, clusterInfo.getMajorVersion());

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ROUTING, "<foobar/>");

        RestUtils.touch(index);
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, index);
        RestUtils.putMapping(index, type, StringUtils.toUTF("{\"_routing\": {\"required\":true}}"));

        runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    //@Test(expected = IOException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test
    public void testSaveWithIngest() throws Exception {
        JobConf conf = createJobConf();

        RestUtils.ExtendedRestClient client = new RestUtils.ExtendedRestClient();
        String prefix = "mroldapi";
        String pipeline = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}";
        client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline));
        client.close();

        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-ingested", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE, "mroldapi-pipeline");
        conf.set(ConfigurationOptions.OPENSEARCH_NODES_INGEST_ONLY, "true");

        runJob(conf);
    }


    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testUpdateWithoutId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-update", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    @Test
    public void testUpsertWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-update", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    @Test(expected = IOException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-updatewoupsert", "data", clusterInfo.getMajorVersion()));

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyScript() throws Exception {
        JobConf conf = createJobConf();
        // use an existing id to allow the update to succeed
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");

        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_RETRY_ON_CONFLICT, "3");

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "int counter = 3");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "int counter = params.param1; String anothercounter = params.param2");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "int counter = params.param1; int anothercounter = params.param2");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScriptWithArray() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwithid", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON, "{ \"some_list\": [\"one\", \"two\"]}");

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "HashSet list = new HashSet(); list.add(ctx._source.list); list.add(params.some_list); ctx._source.list = list.toArray()");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);

        //        conf = createJobConf();
        //        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, "mroldapi/createwithid");
        //        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");
        //
        //        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        //        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        //        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "list = new HashSet(); list.add(ctx._source.picture); list.addAll(some_list); ctx._source.picture = list.toArray()");
        //        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "groovy");
        //        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON, "{ \"some_list\": [\"one\", \"two\"]}");
        //
        //        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScriptWithArrayOnArrayField() throws Exception {
        String docWithArray = "{ \"counter\" : 1 , \"tags\" : [\"an array\", \"with multiple values\"], \"more_tags\" : [ \"I am tag\"], \"even_more_tags\" : \"I am a tag too\" } ";
        String docEndpoint = docEndpoint(indexPrefix + "mroldapi-createwitharray", "data", clusterInfo.getMajorVersion());
        RestUtils.postData(docEndpoint + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi-createwitharray");
        RestUtils.waitForYellow(indexPrefix + "mroldapi-createwitharray");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwitharray", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON, "{ \"new_date\": [\"add me\", \"and me\"]}");

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "HashSet tmp = new HashSet(); tmp.addAll(ctx._source.tags); tmp.addAll(params.new_date); ctx._source.tags = tmp.toArray()");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);
    }


    @Test
    public void testUpsertScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-upsert-script", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "counter = 1");

        runJob(conf);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-upsert-script-param", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS, "param2:name , param3:number, param1:<1>");

        runJob(conf);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-upsert-script-json-param", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        runJob(conf);
    }

    @Test
    public void testUpsertOnlyParamScriptWithArrayOnArrayField() throws Exception {
        String docWithArray = "{ \"counter\" : 1 , \"tags\" : [\"an array\", \"with multiple values\"], \"more_tags\" : [ \"I am tag\"], \"even_more_tags\" : \"I am a tag too\" } ";
        String docEndpoint = docEndpoint(indexPrefix + "mroldapi-createwitharrayupsert", "data", clusterInfo.getMajorVersion());
        RestUtils.postData(docEndpoint + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi-createwitharrayupsert");
        RestUtils.waitForYellow(indexPrefix + "mroldapi-createwitharrayupsert");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-createwitharrayupsert", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.OPENSEARCH_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_PARAMS, (conf.get(ConfigurationOptions.OPENSEARCH_INPUT_JSON).equals("true") ? "update_tags:name" :"update_tags:list"));

        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_INLINE, "ctx._source.tags = params.update_tags");
        conf.set(ConfigurationOptions.OPENSEARCH_UPDATE_SCRIPT_LANG, "painless");

        runJob(conf);
    }


    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-non-existing", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "no");

        runJob(conf);
    }

    @Test
    public void testIndexWithVersionMappingImpliesVersionTypeExternal() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-external-version-implied", "data", clusterInfo.getMajorVersion()));
        // an id must be provided if version type or value are set
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_VERSION, "number");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-pattern-{tag}", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormatting() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-pattern-format-{@timestamp|YYYY-MM-dd}", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormattingAndId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-pattern-format-{@timestamp|YYYY-MM-dd}-with-id", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_MAPPING_ID, "number");

        runJob(conf);
    }

    @Test
    public void testIndexWithEscapedJson() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-simple-escaped-fields", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }


    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, resource("mroldapi-nested", "data", clusterInfo.getMajorVersion()));
        conf.set(ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE, "no");

        RestUtils.putMapping(indexPrefix + "mroldapi-nested", "data", "org/opensearch/hadoop/integration/mr-nested.json");

        runJob(conf);
    }

    private JobConf createJobConf() {
        return new JobConf(config);
    }

    private void runJob(JobConf conf) throws Exception {
        String string = conf.get(ConfigurationOptions.OPENSEARCH_RESOURCE);
        string = indexPrefix + (string.startsWith("/") ? string.substring(1) : string);
        conf.set(ConfigurationOptions.OPENSEARCH_RESOURCE, string);
        JobClient.runJob(conf);
    }
}