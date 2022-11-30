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
package org.opensearch.hadoop.serialization;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.mr.security.HadoopUserProvider;
import org.opensearch.hadoop.rest.InitializationUtils;
import org.opensearch.hadoop.serialization.builder.JdkValueWriter;
import org.opensearch.hadoop.serialization.bulk.BulkCommand;
import org.opensearch.hadoop.serialization.bulk.BulkCommands;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.StringUtils;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class CommandTest {

    private final BytesArray ba = new BytesArray(1024);
    private Object data;
    private final String operation;
    private boolean noId = false;
    private boolean jsonInput = false;
    private final OpenSearchMajorVersion version;

    @Parameters
    public static Collection<Object[]> data() {

        // make sure all versions are tested. Throw if a new one is seen:
        if (OpenSearchMajorVersion.LATEST != OpenSearchMajorVersion.V_3_X) {
            throw new IllegalStateException("CommandTest needs new version updates.");
        }

        Collection<Object[]> result = new ArrayList<>();

        String[] operations = new String[]{ConfigurationOptions.ES_OPERATION_INDEX,
                ConfigurationOptions.ES_OPERATION_CREATE,
                ConfigurationOptions.ES_OPERATION_UPDATE,
                ConfigurationOptions.ES_OPERATION_UPSERT,
                ConfigurationOptions.ES_OPERATION_DELETE};
        boolean[] asJsons = new boolean[]{false, true};
        OpenSearchMajorVersion[] versions = new OpenSearchMajorVersion[]{OpenSearchMajorVersion.V_2_X,
                OpenSearchMajorVersion.V_3_X};

        for (OpenSearchMajorVersion version : versions) {
            for (boolean asJson : asJsons) {
                for (String operation : operations) {
                    result.add(new Object[]{operation, asJson, version});
                }
            }
        }

        return result;
    }

    public CommandTest(String operation, boolean jsonInput, OpenSearchMajorVersion version) {
        this.operation = operation;
        this.jsonInput = jsonInput;
        this.version = version;
    }

    @Before
    public void prepare() {
        ba.reset();
        if (!jsonInput) {
            Map map = new LinkedHashMap();
            map.put("n", 1);
            map.put("s", "v");
            data = map;
        } else {
            data = "{\"n\":1,\"s\":\"v\"}";
        }
    }

    @Test
    public void testNoHeader() throws Exception {
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        assumeFalse(ConfigurationOptions.ES_OPERATION_DELETE.equals(operation));
        create(settings()).write(data).copyTo(ba);
        String result = prefix() + "}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    // check user friendliness and escape the string if needed
    public void testConstantId() throws Exception {
        assumeFalse(isDeleteOP() && jsonInput);
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        Settings settings = settings();
        noId = true;
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<foobar>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_id\":\"foobar\"}}" + map();

        assertEquals(result, ba.toString());
    }

    @Test
    public void testParent() throws Exception {
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        assumeFalse(isDeleteOP() && jsonInput);
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_PARENT, "<5>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"parent\":5}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testVersion() throws Exception {
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        assumeFalse(isDeleteOP() && jsonInput);
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_VERSION, "<3>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"version\":3,\"version_type\":\"external\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTtl() throws Exception {
        assumeFalse(isDeleteOP() && jsonInput);
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_ttl\":2}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTimestamp() throws Exception {
        assumeFalse(isDeleteOP() && jsonInput);
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TIMESTAMP, "<3>");
        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_timestamp\":3}}" + map();
        assertEquals(result, ba.toString());
    }


    @Test
    public void testRouting() throws Exception {
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        assumeFalse(isDeleteOP() && jsonInput);
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "<4>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"routing\":4}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testAllX() throws Exception {
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        assumeFalse(isDeleteOP() && jsonInput);
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "s");

        create(settings).write(data).copyTo(ba);
        String result = "{\"" + operation + "\":{\"_id\":1,\"routing\":\"v\",\"_ttl\":2}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testIdPattern() throws Exception {
        assumeFalse(isDeleteOP() && jsonInput);
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation));
        Settings settings = settings();
        if (version.onOrAfter(OpenSearchMajorVersion.V_2_X)) {
            settings.setResourceWrite("{n}");
        } else {
            settings.setResourceWrite("foo/{n}");
        }

        create(settings).write(data).copyTo(ba);
        String header;
        if (version.onOrAfter(OpenSearchMajorVersion.V_2_X)) {
            header = "{\"_index\":\"1\"" + (isUpdateOp() ? ",\"_id\":2" : "") + "}";
        } else {
            header = "{\"_index\":\"foo\",\"_type\":\"1\"" + (isUpdateOp() ? ",\"_id\":2" : "") + "}";
        }
        String result = "{\"" + operation + "\":" + header + "}" + map();
        assertEquals(result, ba.toString());
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testIdMandatory() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        Settings set = settings();
        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "");
        create(set).write(data).copyTo(ba);
    }

    @Test
    public void testUpdateOnlyInlineScript() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = 3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"retry_on_conflict\":3}}\n" +
                        "{\"script\":{\"source\":\"counter = 3\",\"lang\":\"groovy\"}}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyFileScript() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrAfter(OpenSearchMajorVersion.V_3_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_FILE, "set_count");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"retry_on_conflict\":3}}\n" +
                        "{\"script\":{\"file\":\"set_count\",\"lang\":\"groovy\"}}\n";
        assertEquals(result, ba.toString());
    }

    private BulkCommand create(Settings settings) {
        if (!StringUtils.hasText(settings.getResourceWrite())) {
            settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, operation);
        }
        return BulkCommands.create(settings, null, version);
    }

    private Settings settings() {
        Settings set = new TestSettings();

        set.setInternalVersion(version);
        set.setProperty(ConfigurationOptions.ES_INPUT_JSON, Boolean.toString(jsonInput));
        InitializationUtils.setValueWriterIfNotSet(set, JdkValueWriter.class, null);
        InitializationUtils.setFieldExtractorIfNotSet(set, MapFieldExtractor.class, null);
        InitializationUtils.setBytesConverterIfNeeded(set, JdkBytesConverter.class, null);
        InitializationUtils.setUserProviderIfNotSet(set, HadoopUserProvider.class, null);

        set.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, operation);
        if (version.onOrAfter(OpenSearchMajorVersion.V_2_X)) {
            set.setResourceWrite("foo");
        } else {
            set.setResourceWrite("foo");
        }
        if (isUpdateOp()) {
            set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<2>");
        }
        if (isUpsertOp()) {
            set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<3>");
        }
        return set;
    }

    private String prefix() {
        StringBuilder sb = new StringBuilder("{\"" + operation + "\":{");
        if (isUpdateOp() && !noId) {
            sb.append("\"_id\":2,");
        }
        return sb.toString();
    }

    private String map() {
        if (isDeleteOP()) {
            return "\n";
        }
        StringBuilder sb = new StringBuilder("\n{");
        if (isUpdateOp()) {
            sb.append("\"doc\":{");
        }

        sb.append("\"n\":1,\"s\":\"v\"}");
        if (isUpdateOp()) {
            sb.append("}");
        }
        sb.append("\n");
        return sb.toString();
    }

    private boolean isUpdateOp() {
        return ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation);
    }

    private boolean isUpsertOp() {
        return ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation);
    }

    private boolean isDeleteOP() {
        return ConfigurationOptions.ES_OPERATION_DELETE.equals(operation);
    }
}