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

package org.opensearch.spark.integration;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.StringUtils;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.TestUtils;
import org.opensearch.spark.rdd.api.java.JavaOpenSearchSpark;
import org.opensearch.spark.sql.streaming.api.java.JavaStreamingQueryTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INDEX_AUTO_CREATE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_INGEST_PIPELINE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_MAPPING_EXCLUDE;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_NODES_INGEST_ONLY;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.opensearch.spark.integration.ScalaUtils.propertiesAsScalaMap;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractJavaOpenSearchSparkStructuredStreamingTest {

    private static final transient SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("opensearch-structured-streaming-test")
            .setJars(SparkUtils.OPENSEARCH_SPARK_TESTING_JAR);

    private static transient SparkSession spark = null;

    @Parameterized.Parameters
    public static Collection<Object[]> testParams() {
        Collection<Object[]> params = new ArrayList<>();
        params.add(new Object[] {"java-struct-stream-default"});
        return params;
    }

    @BeforeClass
    public static void setup() {
        conf.setAll(propertiesAsScalaMap(TestSettings.TESTING_PROPS));
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void clean() throws Exception {
        if (spark != null) {
            spark.stop();
            // wait for jetty & spark to properly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    private String prefix;
    private String commitLogDir;
    private OpenSearchMajorVersion version = TestUtils.getOpenSearchClusterInfo().getMajorVersion();

    public AbstractJavaOpenSearchSparkStructuredStreamingTest(String prefix) throws Exception {
        this.prefix = prefix;

        // Set up the commit log directory that we'll use for the test:
        File tempDir = File.createTempFile("opensearch-spark-structured-streaming", "");
        tempDir.delete();
        tempDir.mkdir();
        File logDir = new File(tempDir, "logs");
        logDir.mkdir();
        this.commitLogDir = logDir.getAbsolutePath();
    }

    private String resource(String index, String type) {
        if (TestUtils.isTypelessVersion(version)) {
            return index;
        } else {
            return index + "/" + type;
        }
    }

    private String docPath(String index, String type) {
        if (TestUtils.isTypelessVersion(version)) {
            return index + "/_doc";
        } else {
            return index + "/" + type;
        }
    }

    private String wrapIndex(String index) {
        return prefix + index;
    }

    private String checkpoint(String target) {
        return commitLogDir + "/$target";
    }

    /**
     * Data object for most of the basic tests in here
     */
    public static class RecordBean implements Serializable {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void test0FailOnIndexCreationDisabled() throws Exception {
        String target = wrapIndex(resource("test-nonexisting", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .expectingToThrow(OpenSearchHadoopIllegalArgumentException.class)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(OPENSEARCH_INDEX_AUTO_CREATE, "no")
                        .format("opensearch"),
                target
        );

        assertTrue(!RestUtils.exists(target));
    }

    @Test
    public void test1BasicWrite() throws Exception {
        String target = wrapIndex(resource("test-write", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .format("opensearch"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), containsString("Spark"));
        assertThat(RestUtils.get(target + "/_search?"), containsString("Hadoop"));
        assertThat(RestUtils.get(target + "/_search?"), containsString("YARN"));
    }

    @Test
    public void test1WriteWithMappingId() throws Exception {
        String target = wrapIndex(resource("test-write-id", "data"));
        String docPath = wrapIndex(docPath("test-write-id", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option("opensearch.mapping.id", "id")
                        .format("opensearch"),
                target
        );

        assertEquals(3, JavaOpenSearchSpark.opensearchRDD(new JavaSparkContext(spark.sparkContext()), target).count());
        assertTrue(RestUtils.exists(docPath + "/1"));
        assertTrue(RestUtils.exists(docPath + "/2"));
        assertTrue(RestUtils.exists(docPath + "/3"));

        assertThat(RestUtils.get(target + "/_search?"), containsString("Spark"));
    }

    @Test
    public void test1WriteWithMappingExclude() throws Exception {
        String target = wrapIndex(resource("test-mapping-exclude", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(OPENSEARCH_MAPPING_EXCLUDE, "name")
                        .format("opensearch"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target + "/_search?"), not(containsString("Spark")));
        assertThat(RestUtils.get(target +  "/_search?"), not(containsString("Hadoop")));
        assertThat(RestUtils.get(target +  "/_search?"), not(containsString("YARN")));
    }

    @Test
    public void test2WriteToIngestPipeline() throws Exception {
        String pipelineName =  prefix + "-pipeline";
        String pipeline = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}";
        RestUtils.put("/_ingest/pipeline/" + pipelineName, StringUtils.toUTF(pipeline));

        String target = wrapIndex(resource("test-write-ingest", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("Spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("Hadoop");

        RecordBean doc3 = new RecordBean();
        doc3.setId(3);
        doc3.setName("YARN");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .withInput(doc3)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .option(OPENSEARCH_INGEST_PIPELINE, pipelineName)
                        .option(OPENSEARCH_NODES_INGEST_ONLY, "true")
                        .format("opensearch"),
                target
        );

        assertTrue(RestUtils.exists(target));
        assertThat(RestUtils.get(target+"/_search?"), containsString("\"pipeTEST\":true"));
    }

    @Test
    public void test1MultiIndexWrite() throws Exception {
        String target = wrapIndex(resource("test-write-tech-{name}", "data"));
        JavaStreamingQueryTestHarness<RecordBean> test = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(RecordBean.class));

        RecordBean doc1 = new RecordBean();
        doc1.setId(1);
        doc1.setName("spark");

        RecordBean doc2 = new RecordBean();
        doc2.setId(2);
        doc2.setName("hadoop");

        Dataset<RecordBean> dataset = test
                .withInput(doc1)
                .withInput(doc2)
                .stream();

        test.run(
                dataset.writeStream()
                        .option("checkpointLocation", checkpoint(target))
                        .format("opensearch"),
                target
        );

        assertTrue(RestUtils.exists(wrapIndex(resource("test-write-tech-spark", "data"))));
        assertTrue(RestUtils.exists(wrapIndex(resource("test-write-tech-hadoop", "data"))));

        assertThat(RestUtils.get(wrapIndex(resource("test-write-tech-spark", "data") + "/_search?")), containsString("\"name\":\"spark\""));
        assertThat(RestUtils.get(wrapIndex(resource("test-write-tech-hadoop", "data") + "/_search?")), containsString("\"name\":\"hadoop\""));
    }

    public static class ContactBean implements Serializable {
        private String id;
        private String note;
        private AddressBean address;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getNote() {
            return note;
        }

        public void setNote(String note) {
            this.note = note;
        }

        public AddressBean getAddress() {
            return address;
        }

        public void setAddress(AddressBean address) {
            this.address = address;
        }
    }

    public static class AddressBean implements Serializable {
        private String id;
        private String zipcode;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getZipcode() {
            return zipcode;
        }

        public void setZipcode(String zipcode) {
            this.zipcode = zipcode;
        }
    }

    @Test
    @Ignore("Serialization issues in DataFrameValueWriter when trying to serialize an object for use in parameters")
    public void test3WriteWithUpsertScript() throws Exception {
        // BWC
        String keyword = "keyword";
        String lang = "painless";

        // Init
        String mapping = "{\"data\":{\"properties\":{\"id\":{\"type\":\""+keyword+"\"},\"note\":{\"type\":\""+keyword+"\"},\"address\":{\"type\":\"nested\",\"properties\":{\"id\":{\"type\":\""+keyword+"\"},\"zipcode\":{\"type\":\""+keyword+"\"}}}}}}";
        String index = wrapIndex("test-script-upsert");
        String type = "data";
        String target = resource(index, type);
        String docPath = docPath(index, type);

        RestUtils.touch(index);
        RestUtils.putMapping(index, type, mapping.getBytes());
        RestUtils.postData(docPath+"/1", "{\"id\":\"1\",\"note\":\"First\",\"address\":[]}".getBytes());
        RestUtils.postData(docPath+"/2", "{\"id\":\"2\",\"note\":\"First\",\"address\":[]}".getBytes());

        // Common configurations
        Map<String, String> updateProperties = new HashMap<>();
        updateProperties.put("opensearch.write.operation", "upsert");
        updateProperties.put("opensearch.mapping.id", "id");
        updateProperties.put("opensearch.update.script.lang", lang);

        // Run 1
        ContactBean doc1;
        {
            AddressBean address = new AddressBean();
            address.setId("1");
            address.setZipcode("12345");
            doc1 = new ContactBean();
            doc1.setId("1");
            doc1.setAddress(address);
        }

        String script1 = "ctx._source.address.add(params.new_address)";

        JavaStreamingQueryTestHarness<ContactBean> test1 = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(ContactBean.class));

        test1
            .withInput(doc1)
            .run(
                test1.stream()
                    .writeStream()
                    .option("checkpointLocation", checkpoint(target))
                    .options(updateProperties)
                    .option("opensearch.update.script.params", "new_address:address")
                    .option("opensearch.update.script", script1)
                    .format("opensearch"),
                target
            );

        // Run 2
        ContactBean doc2;
        {
            doc2 = new ContactBean();
            doc2.setId("2");
            doc2.setNote("Second");
        }

        String script2 = "ctx._source.note = params.new_note";

        JavaStreamingQueryTestHarness<ContactBean> test2 = new JavaStreamingQueryTestHarness<>(spark, Encoders.bean(ContactBean.class));

        test2
            .withInput(doc2)
            .run(
                test2.stream()
                    .writeStream()
                    .option("checkpointLocation", checkpoint(target))
                    .options(updateProperties)
                    .option("opensearch.update.script.params", "new_note:note")
                    .option("opensearch.update.script", script2)
                    .format("opensearch"),
                target
            );

        // Validate
        assertTrue(RestUtils.exists(docPath + "/1"));
        assertThat(RestUtils.get(docPath + "/1"), both(containsString("\"zipcode\":\"12345\"")).and(containsString("\"note\":\"First\"")));

        assertTrue(RestUtils.exists(docPath + "/2"));
        assertThat(RestUtils.get(docPath + "/2"), both(not(containsString("\"zipcode\":\"12345\""))).and(containsString("\"note\":\"Second\"")));
    }
}