/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.NoOpLog;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.PropertiesSettings;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.ClusterInfo;
import org.opensearch.hadoop.util.FastByteArrayInputStream;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;

import java.util.List;

import static org.junit.Assert.*;

public class ServerlessModeTest {

    private static final Log LOGGER = new NoOpLog("ServerlessModeTest");

    @Test
    public void testServerlessModeSettingDefault() {
        Settings settings = new PropertiesSettings();
        assertFalse(settings.getServerlessMode());
    }

    @Test
    public void testServerlessModeSettingEnabled() {
        Settings settings = new PropertiesSettings();
        settings.setServerlessMode(true);
        assertTrue(settings.getServerlessMode());
    }

    @Test
    public void testServerlessModeSettingDisabled() {
        Settings settings = new PropertiesSettings();
        settings.setServerlessMode(false);
        assertFalse(settings.getServerlessMode());
    }

    @Test
    public void testFindServerlessPartitionsSingleIndex() {
        Settings settings = new PropertiesSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_RESOURCE_READ, "test-index");
        settings.setServerlessMode(true);

        List<PartitionDefinition> partitions = RestService.findServerlessPartitions(null, settings, null, LOGGER);

        assertEquals(1, partitions.size());
        assertEquals("test-index", partitions.get(0).getIndex());
        assertEquals(0, partitions.get(0).getShardId());
    }

    @Test
    public void testFindServerlessPartitionsMultipleIndices() {
        Settings settings = new PropertiesSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_RESOURCE_READ, "index1,index2,index3");
        settings.setServerlessMode(true);

        List<PartitionDefinition> partitions = RestService.findServerlessPartitions(null, settings, null, LOGGER);

        assertEquals(3, partitions.size());
        assertEquals("index1", partitions.get(0).getIndex());
        assertEquals("index2", partitions.get(1).getIndex());
        assertEquals("index3", partitions.get(2).getIndex());
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void testFindServerlessPartitionsRejectsMaxDocsPerPartition() {
        Settings settings = new PropertiesSettings();
        settings.setProperty(ConfigurationOptions.OPENSEARCH_RESOURCE_READ, "test-index");
        settings.setServerlessMode(true);
        settings.setMaxDocsPerPartition(1000);

        RestService.findServerlessPartitions(null, settings, null, LOGGER);
    }

    @Test
    public void testRefreshSkippedInServerlessMode() throws Exception {
        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Settings settings = new TestSettings();
        settings.setServerlessMode(true);
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        settings.setResourceWrite("test-index");

        RestClient client = new RestClient(settings, mock);
        Resource resource = new Resource(settings, false);

        client.refresh(resource);

        // Verify no network call was made
        Mockito.verifyZeroInteractions(mock);
    }

    @Test
    public void testMainInfoReturnsServerlessClusterInfo() throws Exception {
        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Settings settings = new TestSettings();
        settings.setServerlessMode(true);

        RestClient client = new RestClient(settings, mock);
        ClusterInfo info = client.mainInfo();

        assertEquals("serverless-collection", info.getClusterName().getName());
        assertEquals("serverless-uuid", info.getClusterName().getUUID());
        assertEquals(OpenSearchMajorVersion.V_2_X, info.getMajorVersion());
        Mockito.verifyZeroInteractions(mock);
    }

    @Test
    public void testMainInfoNormalMode() throws Exception {
        String response = "{\n" +
                "\"name\": \"node\",\n" +
                "\"cluster_name\": \"my-cluster\",\n" +
                "\"cluster_uuid\": \"abc123\",\n" +
                "\"version\": {\n" +
                "  \"number\": \"2.4.0\"\n" +
                "},\n" +
                "\"tagline\": \"The OpenSearch Project: https://opensearch.org/\"\n" +
                "}";

        NetworkClient mock = Mockito.mock(NetworkClient.class);
        Mockito.when(mock.execute(Mockito.any(SimpleRequest.class), Mockito.eq(true)))
                .thenReturn(new SimpleResponse(200, new FastByteArrayInputStream(new BytesArray(response)), "localhost:9200"));

        Settings settings = new TestSettings();
        settings.setServerlessMode(false);

        RestClient client = new RestClient(settings, mock);
        ClusterInfo info = client.mainInfo();

        assertEquals("my-cluster", info.getClusterName().getName());
        assertEquals("abc123", info.getClusterName().getUUID());
    }
}
