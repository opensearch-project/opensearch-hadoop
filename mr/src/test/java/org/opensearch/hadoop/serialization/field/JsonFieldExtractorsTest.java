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

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.opensearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class JsonFieldExtractorsTest {

    @Test
    public void indexAndType() {
        Settings settings = new TestSettings();
        // Types will not be supported in 8.x
        settings.setInternalVersion(OpenSearchMajorVersion.V_7_X);
        settings.setResourceWrite("test/{field}");
        JsonFieldExtractors jsonFieldExtractors = new JsonFieldExtractors(settings);

        String data = "{\"field\":\"data\"}";
        BytesArray bytes = new BytesArray(data);

        jsonFieldExtractors.process(bytes);

        assertThat(jsonFieldExtractors.indexAndType().hasPattern(), is(true));
        assertThat(jsonFieldExtractors.indexAndType().field(data).toString(), equalTo("\"_index\":\"test\",\"_type\":\"data\""));
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void indexAndTypeNull() {
        Settings settings = new TestSettings();
        settings.setInternalVersion(OpenSearchMajorVersion.LATEST);
        settings.setResourceWrite("test/{optional}");
        JsonFieldExtractors jsonFieldExtractors = new JsonFieldExtractors(settings);

        String data = "{\"field\":null}";
        BytesArray bytes = new BytesArray(data);

        jsonFieldExtractors.process(bytes);

        assertThat(jsonFieldExtractors.indexAndType().hasPattern(), is(true));
        jsonFieldExtractors.indexAndType().field(data);
        fail();
    }

    @Test(expected = OpenSearchHadoopIllegalArgumentException.class)
    public void indexAndTypeFailure() {
        Settings settings = new TestSettings();
        settings.setInternalVersion(OpenSearchMajorVersion.LATEST);
        settings.setResourceWrite("test/{optional}");
        JsonFieldExtractors jsonFieldExtractors = new JsonFieldExtractors(settings);

        String data = "{\"field\":\"data\"}";
        BytesArray bytes = new BytesArray(data);

        jsonFieldExtractors.process(bytes);

        assertThat(jsonFieldExtractors.indexAndType().hasPattern(), is(true));
        jsonFieldExtractors.indexAndType().field(data);
        fail();
    }
}