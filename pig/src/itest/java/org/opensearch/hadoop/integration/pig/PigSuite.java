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
package org.opensearch.hadoop.integration.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.io.FileLocalizer;
import org.opensearch.hadoop.TestData;
import org.opensearch.hadoop.fs.HdfsUtils;
import org.opensearch.hadoop.fixtures.LocalEs;
import org.opensearch.hadoop.Provisioner;
import org.opensearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractPigSaveTest.class, AbstractPigSaveJsonTest.class, AbstractPigSearchTest.class, AbstractPigSearchJsonTest.class, AbstractPigReadAsJsonTest.class, AbstractPigExtraTests.class })
//@Suite.SuiteClasses({ AbstractPigSaveTest.class, AbstractPigSearchTest.class })
//@Suite.SuiteClasses({ AbstractPigExtraTests.class })
public class PigSuite {

    static {
        if (TestUtils.isWindows()) {
            FileLocalizer.OWNER_ONLY_PERMS.fromShort((short) 0650);
        }
    }

    @ClassRule
    public static ExternalResource resource = new LocalEs();

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @ClassRule
    public static TestData testData = new TestData();

    @BeforeClass
    public static void setup() {
        HdfsUtils.copyFromLocal(Provisioner.OPENSEARCHHADOOP_TESTING_JAR, Provisioner.HDFS_ES_HDP_LIB);
        HdfsUtils.rmr(new Configuration(), "tmp-pig");
    }
}