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

import java.io.IOException;
import java.util.List;

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.RestClient.Health;
import org.opensearch.hadoop.serialization.dto.mapping.Mapping;
import org.opensearch.hadoop.serialization.dto.mapping.MappingSet;
import org.opensearch.hadoop.util.ByteSequence;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.IOUtils;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.StringUtils;
import org.opensearch.hadoop.util.TestSettings;
import org.opensearch.hadoop.util.TestUtils;
import org.opensearch.hadoop.util.unit.TimeValue;

import static org.opensearch.hadoop.rest.Request.Method.*;

public class RestUtils {

    public static class ExtendedRestClient extends RestClient {

        private static ClusterInfo TEST_CLUSTER = null;

        private static Settings withVersion(Settings settings) {
            if (TEST_CLUSTER == null) {
                TEST_CLUSTER = TestUtils.getOpenSearchClusterInfo();
            }
            settings.setInternalClusterInfo(TEST_CLUSTER);
            return settings;
        }

        public ExtendedRestClient() {
            super(withVersion(new TestSettings()));
        }

        @Override
        public Response execute(Request.Method method, String path, ByteSequence buffer) {
            return super.execute(method, path, buffer);
        }

        public String get(String index) throws IOException {
            return IOUtils.asString(execute(Request.Method.GET, index));
        }

        public String post(String index, byte[] buffer) throws IOException {
            return IOUtils.asString(execute(Request.Method.POST, index, new BytesArray(buffer)).body());
        }

        public String put(String index, byte[] buffer) throws IOException {
            return IOUtils.asString(execute(PUT, index, new BytesArray(buffer)).body());
        }

        public String refresh(String index) throws IOException {
            return IOUtils.asString(execute(Request.Method.POST, index + "/_refresh", (ByteSequence) null).body());
        }
    }

    public static void putMapping(String index, String type, byte[] content) throws Exception {
        RestClient rc = new ExtendedRestClient();
        BytesArray bs = new BytesArray(content);
        rc.putMapping(index, type, bs.bytes());
        rc.close();
    }

    public static Mapping getMapping(String index, String type) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        MappingSet parseField = rc.getMappings(index + "/_mapping/" + type, true);
        rc.close();
        return parseField.getMapping(index, type);
    }

    public static MappingSet getMappings(String index) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        MappingSet parseField = rc.getMappings(index + "/_mapping", false);
        rc.close();
        return parseField;
    }

    public static String get(String index) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        String str = rc.get(index);
        rc.close();
        return str;
    }

    public static void putMapping(String index, String type, String location) throws Exception {
        putMapping(index, type, TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location)));
    }

    public static void postData(String index, String location) throws Exception {
        byte[] fromInputStream = TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location));
        postData(index, fromInputStream);
    }

    public static void postData(String index, byte[] content) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.post(index, content);
        rc.close();
    }

    public static void put(String index, byte[] content) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.put(index, content);
        rc.close();
    }

    public static void delete(String index) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.delete(index);
        rc.close();
    }

    public static void bulkData(String index, String location) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.put(index + "/_bulk", TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location)));
        rc.close();
    }

    public static void waitForYellow(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.waitForHealth(string, Health.YELLOW, TimeValue.timeValueSeconds(5));
        rc.close();
    }

    public static void refresh(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.refresh(string);
        rc.close();
    }

    public static boolean exists(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        boolean result;
        List<String> parts = StringUtils.tokenize(string, "/");
        if (parts.size() == 1) {
            result = rc.indexExists(string);
        } else if (parts.size() == 2) {
            result = rc.typeExists(parts.get(0), parts.get(1));
        } else if (parts.size() == 3) {
            result = rc.documentExists(parts.get(0), parts.get(1), parts.get(2));
        } else {
            throw new RuntimeException("Invalid exists path : " + string);
        }
        rc.close();
        return result;
    }

    public static boolean touch(String index) throws IOException {
        ExtendedRestClient rc = new ExtendedRestClient();
        boolean result = rc.touch(index);
        rc.close();
        return result;
    }
}