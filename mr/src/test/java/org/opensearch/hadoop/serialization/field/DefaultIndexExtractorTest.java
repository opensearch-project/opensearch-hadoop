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

package org.opensearch.hadoop.serialization.field;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.InitializationUtils;
import org.opensearch.hadoop.rest.Resource;
import org.opensearch.hadoop.serialization.MapFieldExtractor;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.ObjectUtils;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DefaultIndexExtractorTest {

    private static final Log LOG = LogFactory.getLog(DefaultIndexExtractorTest.class);

    @Test
    public void createFieldExtractor() {
        Settings settings = new TestSettings();
        settings.setResourceWrite("{field}");
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapFieldExtractor.class, LOG);

        IndexExtractor iformat = ObjectUtils.instantiate(settings.getMappingIndexExtractorClassName(), settings);
        iformat.compile(new Resource(settings, false).toString());

        assertThat(iformat.hasPattern(), is(true));

        Map<String, String> data = new HashMap<String, String>();
        data.put("field", "data");

        Object field = iformat.field(data);
        assertThat(field.toString(), equalTo("\"_index\":\"data\""));
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void createFieldExtractorNull() {
        Settings settings = new TestSettings();
        settings.setResourceWrite("test/{field}");
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapFieldExtractor.class, LOG);

        IndexExtractor iformat = ObjectUtils.instantiate(settings.getMappingIndexExtractorClassName(), settings);
        iformat.compile(new Resource(settings, false).toString());

        assertThat(iformat.hasPattern(), is(true));

        Map<String, String> data = new HashMap<String, String>();
        data.put("field", null);

        iformat.field(data);

        fail();
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void createFieldExtractorFailure() {
        Settings settings = new TestSettings();
        settings.setResourceWrite("test/{optional}");
        settings.setInternalVersion(OpenSearchMajorVersion.V_2_X);
        InitializationUtils.setFieldExtractorIfNotSet(settings, MapFieldExtractor.class, LOG);

        IndexExtractor iformat = ObjectUtils.instantiate(settings.getMappingIndexExtractorClassName(), settings);
        iformat.compile(new Resource(settings, false).toString());

        assertThat(iformat.hasPattern(), is(true));

        Map<String, String> data = new HashMap<String, String>();
        data.put("field", "data");

        iformat.field(data);

        fail();
    }
}