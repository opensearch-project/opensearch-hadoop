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
package org.opensearch.hadoop.util;

import java.io.IOException;
import java.util.Properties;

import org.opensearch.hadoop.cfg.PropertiesSettings;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_RESOURCE;

/**
 * Tweaked settings for testing.
 */
public class TestSettings extends PropertiesSettings {

    public final static Properties TESTING_PROPS = new Properties();

    static {
        // pick up system properties
        TESTING_PROPS.putAll(System.getProperties());

        // override with test settings
        try {
            TESTING_PROPS.load(TestUtils.class.getResourceAsStream("/test.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Cannot load default Hadoop test properties");
        }

        // manually select the hadoop properties
        String fs = System.getProperty("hd.fs");
        String jt = System.getProperty("hd.jt");

        // override
        if (StringUtils.hasText(fs)) {
            System.out.println("Setting FS to " + fs);
            TESTING_PROPS.put("fs.default.name", fs.trim());
        }

        if (StringUtils.hasText(jt)) {
            System.out.println("Setting JT to " + jt);
            TESTING_PROPS.put("mapred.job.tracker", jt.trim());
        }
    }

    {
        // pick up dedicated OpenSearch port if present
        String embeddedOpenSearchLocalPort = System.getProperty(TestUtils.OPENSEARCH_LOCAL_PORT);
        if (StringUtils.hasText(System.getProperty(TestUtils.OPENSEARCH_LOCAL_PORT))) {
            TESTING_PROPS.put("opensearch.port", embeddedOpenSearchLocalPort);
        }
    }

    public TestSettings() {
        super(new Properties());
        getProperties().putAll(TESTING_PROPS);
    }

    public TestSettings(String uri) {
        this();
        setProperty(OPENSEARCH_RESOURCE, uri);
    }

    public Properties getProperties() {
        return props;
    }
}