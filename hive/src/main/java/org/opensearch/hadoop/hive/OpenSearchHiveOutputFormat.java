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
package org.opensearch.hadoop.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;
import org.opensearch.hadoop.cfg.HadoopSettingsManager;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.mr.OpenSearchOutputFormat;
import org.opensearch.hadoop.mr.security.HadoopUserProvider;
import org.opensearch.hadoop.rest.InitializationUtils;

/**
 * Hive specific OutputFormat.
 */
@SuppressWarnings("rawtypes")
public class OpenSearchHiveOutputFormat extends OpenSearchOutputFormat implements HiveOutputFormat {

    static class OpenSearchHiveRecordWriter extends OpenSearchRecordWriter implements RecordWriter {

        private final Progressable progress;

        public OpenSearchHiveRecordWriter(Configuration cfg, Progressable progress) {
            super(cfg, progress);
            this.progress = progress;
        }

        @Override
        public void write(Writable w) throws IOException {
            if (!initialized) {
                initialized = true;
                init();
            }

            if (w instanceof HiveBytesArrayWritable) {
                HiveBytesArrayWritable hbaw = ((HiveBytesArrayWritable) w);
                repository.writeProcessedToIndex(hbaw.getContent());
            }
            else {
                // we could allow custom BAs
                throw new OpenSearchHadoopIllegalArgumentException(String.format("Unexpected type; expected [%s], received [%s]", HiveBytesArrayWritable.class, w));
            }
        }

        @Override
        public void close(boolean abort) throws IOException {
            // TODO: check whether a proper Reporter can be passed in
            super.doClose(progress);
        }
    }

    public OpenSearchHiveRecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) {
        // force the table properties to be merged into the configuration
        // NB: the properties are also available in HiveConstants#OUTPUT_TBL_PROPERTIES
        Settings settings = HadoopSettingsManager.loadFrom(jc).merge(tableProperties);

        Log log = LogFactory.getLog(getClass());

        // NB: ESSerDe is already initialized at this stage but should still have a reference to the same cfg object
        // NB: the value writer is not needed by Hive but it's set for consistency and debugging purposes

        InitializationUtils.setValueWriterIfNotSet(settings, HiveValueWriter.class, log);
        InitializationUtils.setBytesConverterIfNeeded(settings, HiveBytesConverter.class, log);
        InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, log);

        // set write resource
        settings.setResourceWrite(settings.getResourceWrite());

        HiveUtils.init(settings, log);

        return new OpenSearchHiveRecordWriter(jc, progress);
    }
}