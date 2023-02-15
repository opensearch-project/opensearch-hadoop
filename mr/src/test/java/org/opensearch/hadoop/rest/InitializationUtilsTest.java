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

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.opensearch.hadoop.cfg.ConfigurationOptions.*;
import static org.opensearch.hadoop.rest.InitializationUtils.*;

public class InitializationUtilsTest {

    @Test
    public void testValidateDefaultSettings() throws Exception {
        Settings set = new TestSettings();
        validateSettings(set);

        assertFalse(set.getNodesWANOnly());
        assertTrue(set.getNodesDiscovery());
        assertTrue(set.getNodesDataOnly());
        assertFalse(set.getNodesClientOnly());
    }

    @Test
    public void testValidateWANOnly() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_WAN_ONLY, "true");
        validateSettings(set);

        assertTrue(set.getNodesWANOnly());
        assertFalse(set.getNodesDiscovery());
        assertFalse(set.getNodesDataOnly());
        assertFalse(set.getNodesClientOnly());
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateWANOnlyWithDiscovery() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_WAN_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_DISCOVERY, "true");
        validateSettings(set);
    }

    @Test
    public void testValidateClientOnlyNodesWithDefaultData() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_CLIENT_ONLY, "true");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDefaultDataVsClientOnlyNodes() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_CLIENT_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_DATA_ONLY, "true");
        validateSettings(set);
    }

    @Test
    public void testValidateIngestOnlyNodesWithDefaults() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_INGEST_ONLY, "true");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateIngestOnlyVsDataOnly() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_INGEST_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_DATA_ONLY, "true");
        validateSettings(set);
    }


    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateIngestOnlyVsClientOnly() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_INGEST_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_CLIENT_ONLY, "true");
        validateSettings(set);
    }


    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateAllRestrictionsBreak() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_NODES_CLIENT_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_DATA_ONLY, "true");
        set.setProperty(OPENSEARCH_NODES_INGEST_ONLY, "true");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateMultipleScripts() throws Exception {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_UPDATE_SCRIPT_FILE, "test");
        set.setProperty(OPENSEARCH_UPDATE_SCRIPT_INLINE, "test");
        set.setProperty(OPENSEARCH_UPDATE_SCRIPT_STORED, "test");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateWriteTTLRemoved() throws Exception {
        Settings set = new TestSettings();
        set.setInternalClusterInfo(ClusterInfo.unnamedClusterWithVersion(OpenSearchMajorVersion.V_1_X));
        set.setProperty(OPENSEARCH_MAPPING_TTL, "1000");
        validateSettingsForWriting(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateWriteTimestampRemoved() throws Exception {
        Settings set = new TestSettings();
        set.setInternalClusterInfo(ClusterInfo.unnamedClusterWithVersion(OpenSearchMajorVersion.V_1_X));
        set.setProperty(OPENSEARCH_MAPPING_TIMESTAMP, "1000");
        validateSettingsForWriting(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDeleteOperationVsInputAsJson() {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_WRITE_OPERATION, "delete");
        set.setProperty(OPENSEARCH_INPUT_JSON, "true");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDeleteOperationVsIncludeFields() {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_WRITE_OPERATION, "delete");
        set.setProperty(OPENSEARCH_MAPPING_INCLUDE, "field");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDeleteOperationVsExcludeFields() {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_WRITE_OPERATION, "delete");
        set.setProperty(OPENSEARCH_MAPPING_EXCLUDE, "field");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDeleteOperationVsIdNotSet() {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_WRITE_OPERATION, "delete");
        validateSettings(set);
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testValidateDeleteOperationVsEmptyId() {
        Settings set = new TestSettings();
        set.setProperty(OPENSEARCH_WRITE_OPERATION, "delete");
        set.setProperty(OPENSEARCH_MAPPING_ID, "");
        validateSettings(set);
    }
}