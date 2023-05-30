/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.cfg;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class SettingsTest {
    @Test
    public void getXOpaqueId() throws Exception {
        TestSettings testSettings = new TestSettings();
        String opaqueId1 = "This is an opaque ID";
        testSettings.setOpaqueId(opaqueId1);
        Assert.assertEquals(opaqueId1, testSettings.getOpaqueId());
        testSettings.setOpaqueId("This one\n has newlines\r\n and a carriage return");
        Assert.assertEquals("This one has newlines and a carriage return", testSettings.getOpaqueId());
        testSettings.setOpaqueId("This oñe has a non-ascii character");
        Assert.assertEquals("This oe has a non-ascii character", testSettings.getOpaqueId());
    }

    public static class TestSettings extends Settings {
        private Map<String, String> actualSettings = new HashMap();
        @Override
        public InputStream loadResource(String location) {
            return null;
        }

        @Override
        public Settings copy() {
            return null;
        }

        @Override
        public String getProperty(String name) {
            return actualSettings.get(name);
        }

        @Override
        public void setProperty(String name, String value) {
            actualSettings.put(name, value);
        }

        @Override
        public Properties asProperties() {
            return null;
        }
    }
}