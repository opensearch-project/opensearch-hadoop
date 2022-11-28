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

import java.util.List;
import java.util.Properties;

import org.opensearch.hadoop.cfg.PropertiesSettings;
import org.opensearch.hadoop.serialization.field.FieldFilter;
import org.junit.Test;

import static org.junit.Assert.assertThat;

import static org.hamcrest.core.IsEqual.equalTo;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_NODES;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_NODES_DISCOVERY;
import static org.opensearch.hadoop.cfg.ConfigurationOptions.OPENSEARCH_PORT;

public class SettingsUtilsTest {

    @Test
    public void testHostWithoutAPortFallingBackToDefault() throws Exception {
        Properties props = new Properties();
        props.setProperty(OPENSEARCH_NODES, "localhost");
        props.setProperty("opensearch.port", "9800");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithoutAPortFallingBackToDefaultAndNoDiscovery() throws Exception {
        Properties props = new Properties();
        props.setProperty(OPENSEARCH_NODES, "localhost");
        props.setProperty(OPENSEARCH_PORT, "9800");
        props.setProperty(OPENSEARCH_NODES_DISCOVERY, "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithAPortAndFallBack() throws Exception {
        Properties props = new Properties();
        props.setProperty(OPENSEARCH_NODES, "localhost:9800");
        props.setProperty(OPENSEARCH_PORT, "9300");
        props.setProperty(OPENSEARCH_NODES_DISCOVERY, "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithoutAPortFallingBackToDefaultAndNoDiscoveryWithSchema() throws Exception {
        Properties props = new Properties();
        props.setProperty(OPENSEARCH_NODES, "http://localhost");
        props.setProperty(OPENSEARCH_PORT, "9800");
        props.setProperty(OPENSEARCH_NODES_DISCOVERY, "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("http://127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testHostWithAPortAndFallBackWithSchema() throws Exception {
        Properties props = new Properties();
        props.setProperty(OPENSEARCH_NODES, "http://localhost:9800");
        props.setProperty(OPENSEARCH_PORT, "9300");
        props.setProperty(OPENSEARCH_NODES_DISCOVERY, "false");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        assertThat(nodes.size(), equalTo(1));
        assertThat("http://127.0.0.1:9800", equalTo(nodes.get(0)));
    }

    @Test
    public void testGetArrayIncludes() throws Exception {
        Properties props = new Properties();
        props.setProperty("es.read.field.as.array.include", "a:4");

        PropertiesSettings settings = new PropertiesSettings(props);
        List<FieldFilter.NumberedInclude> filters = SettingsUtils.getFieldArrayFilterInclude(settings);
        assertThat(filters.size(), equalTo(1));
        assertThat(filters.get(0), equalTo(new FieldFilter.NumberedInclude("a", 4)));
    }
}