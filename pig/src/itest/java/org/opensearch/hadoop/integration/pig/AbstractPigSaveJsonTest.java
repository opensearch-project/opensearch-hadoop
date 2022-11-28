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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.opensearch.hadoop.OpenSearchHadoopIllegalStateException;
import org.opensearch.hadoop.HdpBootstrap;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.OpenSearchAssume;
import org.opensearch.hadoop.rest.RestUtils;
import org.opensearch.hadoop.rest.RestClient;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.opensearch.hadoop.util.TestUtils.resource;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractPigSaveJsonTest extends AbstractPigTests {

    private final OpenSearchMajorVersion VERSION = TestUtils.getOpenSearchClusterInfo().getMajorVersion();
    private final Configuration configuration = HdpBootstrap.hadoopConfig();

    @BeforeClass
    public static void startup() throws Exception {
        AbstractPigTests.startup();
        // initialize Pig in local mode
        RestClient client = new RestClient(new TestSettings());
        try {
            client.delete("json-pig*");
        } catch (Exception ex) {
            // ignore
        }
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "SET mapred.map.tasks 2;" +
                loadSource() +
                //"ILLUSTRATE A;" +
                "STORE A INTO '"+resource("json-pig-tupleartists", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('es.input.json=true');";
        //"es_total = LOAD 'radio/artists/_count?q=me*' USING org.opensearch.pig.hadoop.OpenSearchStorage();" +
        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-fieldalias", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('es.input.json=true','es.mapping.names=data:@json');";

        pig.executeScript(script);
    }

    @Test
    public void testCreateWithId() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-createwithid", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=create','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test(expected = OpenSearchHadoopIllegalStateException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test(expected = Exception.class)
    public void testUpdateWithoutId() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-updatewoid", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-update", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=upsert','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test(expected = OpenSearchHadoopIllegalStateException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-updatewoupsert", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test
    public void testIndexPattern() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-pattern-{tag}", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('es.input.json=true');";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternFormat() throws Exception {
        String script =
                loadSource() +
                "STORE A INTO '"+resource("json-pig-pattern-format-{@timestamp|YYYY-MM-dd}", "data", VERSION)+"' USING org.opensearch.pig.hadoop.OpenSearchStorage('es.input.json=true');";

        pig.executeScript(script);
    }

    private String loadSource() throws IOException {
        return "A = LOAD '" + PigSuite.testData.sampleArtistsJson(configuration) + "' USING PigStorage() AS (json: chararray);";
    }
}