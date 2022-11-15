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
package org.opensearch.spark.sql;

import org.elasticsearch.spark.sql.Utils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void testCamelCase() throws Exception {
        Assert.assertEquals("foo.bar", Utils.camelCaseToDotNotation("foo.bar"));
        assertEquals("foo.bar", Utils.camelCaseToDotNotation("fooBar"));
        assertEquals("foo.bar", Utils.camelCaseToDotNotation("fooBAr"));
        assertEquals("foo", Utils.camelCaseToDotNotation("FOO"));
        assertEquals("foo.bar", Utils.camelCaseToDotNotation("FOO.BAR"));
        assertEquals("foo.bar", Utils.camelCaseToDotNotation("FOO.BAR"));
        assertEquals("es.port", Utils.camelCaseToDotNotation("esPort"));
    }
}