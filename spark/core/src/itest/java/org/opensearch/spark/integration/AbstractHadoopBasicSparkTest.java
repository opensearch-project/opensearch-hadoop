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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoRegistrator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.hadoop.TestData;
import org.opensearch.hadoop.util.TestSettings;

import com.esotericsoftware.kryo.Kryo;

public class AbstractHadoopBasicSparkTest implements Serializable {

    private transient final SparkConf conf = new SparkConf()
            .setAppName("basictest")
            .set("spark.io.compression.codec", "lz4")
            .setAll(ScalaUtils.propertiesAsScalaMap(TestSettings.TESTING_PROPS));
    private transient SparkConf cfg = null;
    private transient JavaSparkContext sc;

    @ClassRule
    public static TestData testData = new TestData();

    @Before
    public void setup() {
        cfg = conf.clone();
    }

    @After
    public void clean() throws Exception {
        if (sc != null) {
            sc.stop();
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    @Test
    public void testBasicRead() throws Exception {
        sc = new JavaSparkContext(cfg);
        JavaRDD<String> data = readAsRDD(testData.sampleArtistsDatUri()).cache();

        assertThat((int) data.count(), is(greaterThan(300)));

        long radioHead = data.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) { return s.contains("Radiohead"); }
        }).count();

        assertThat((int) radioHead, is(1));
        assertEquals(1, radioHead);

        long megadeth = data.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) { return s.contains("Megadeth"); }
        }).count();

        assertThat((int) megadeth, is(1));
    }

    public static class MyRegistrator implements Serializable, KryoRegistrator {

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(Text.class);
            kryo.register(MapWritable.class);
        }
    }
    
    private JavaRDD<String> readAsRDD(URI uri) throws Exception {
        // don't use the sc.read.json/textFile to avoid the whole Hadoop madness
        Path path = Paths.get(uri);
        return sc.parallelize(Files.readAllLines(path, StandardCharsets.ISO_8859_1));
   }
}