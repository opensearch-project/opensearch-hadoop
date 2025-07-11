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

package org.opensearch.hadoop.gradle.fixture.hadoop.services

import org.opensearch.gradle.Version
import org.opensearch.hadoop.gradle.fixture.hadoop.ConfigFormats
import org.opensearch.hadoop.gradle.fixture.hadoop.RoleDescriptor
import org.opensearch.hadoop.gradle.fixture.hadoop.ServiceDescriptor
import org.opensearch.hadoop.gradle.fixture.hadoop.SetupTaskFactory
import org.opensearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.opensearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.opensearch.hadoop.gradle.fixture.hadoop.conf.ServiceConfiguration

import static org.opensearch.hadoop.gradle.fixture.hadoop.conf.SettingsContainer.FileSettings

class SparkYarnServiceDescriptor implements ServiceDescriptor {

    static RoleDescriptor GATEWAY = RoleDescriptor.requiredGateway('spark', [])

    @Override
    String id() {
        return 'spark'
    }

    @Override
    String serviceName() {
        return 'spark'
    }

    @Override
    List<ServiceDescriptor> serviceDependencies() {
        return [HadoopClusterConfiguration.HADOOP]
    }

    @Override
    List<RoleDescriptor> roles() {
        return [GATEWAY]
    }

    @Override
    Version defaultVersion() {
        return new Version(3, 4, 4)
    }

    String hadoopVersionCompatibility() {
        // The spark artifacts that interface with Hadoop have a hadoop version in their names.
        // This version is not always a version that Hadoop still distributes.
        return "3"
    }

    @Override
    String getDependencyCoordinates(ServiceConfiguration configuration) {
        return "apache:spark:${configuration.getVersion()}@tgz"
    }

    @Override
    String packageName() {
        return 'spark'
    }

    @Override
    String artifactName(ServiceConfiguration configuration) {
        Version version = configuration.getVersion()
        return "spark-$version-bin-hadoop${hadoopVersionCompatibility()}"
    }

    @Override
    Collection<String> excludeFromArchiveExtraction(InstanceConfiguration configuration) {
        // It's nice all these projects have example data, but we'll scrap it all for now. I don't think we'll need to
        // run anything on kubernetes for a while. Might bring R back in if we ever deem it necessary to test on.
        String rootName = artifactName(configuration.serviceConf)
        return ["$rootName/data/", "$rootName/examples/", "$rootName/kubernetes/", "$rootName/R/"]
    }

    @Override
    String homeDirName(InstanceConfiguration configuration) {
        return artifactName(configuration.getServiceConf())
    }

    @Override
    String pidFileName(InstanceConfiguration configuration) {
        return 'spark.pid'
    }

    @Override
    String confDirName(InstanceConfiguration configuration) {
        return 'conf'
    }

    @Override
    List<String> configFiles(InstanceConfiguration configuration) {
        return ['spark-defaults.conf']
    }

    @Override
    Map<String, FileSettings> collectConfigFilesContents(InstanceConfiguration configuration) {
        return ['spark-defaults.conf' : configuration.getSettingsContainer().flattenFile('spark-defaults.conf')]
    }

    @Override
    Closure<String> configFormat(InstanceConfiguration configuration) {
        return ConfigFormats.whiteSpaced()
    }

    @Override
    String httpUri(InstanceConfiguration configuration, Map<String, FileSettings> configFileContents) {
        if (GATEWAY.equals(configuration.roleDescriptor)) {
            return null
        }
        throw new UnsupportedOperationException("Unknown instance [${configuration.roleDescriptor.roleName()}]")
    }

    @Override
    List<String> startCommand(InstanceConfiguration configuration) {
        // No start command for gateway services
        return ['']
    }

    @Override
    String scriptDir(InstanceConfiguration instance) {
        return 'bin'
    }

    @Override
    String javaOptsEnvSetting(InstanceConfiguration configuration) {
        return ''
    }

    @Override
    void finalizeEnv(Map<String, String> env, InstanceConfiguration configuration) {
        // Get Hadoop Conf Dir
        InstanceConfiguration hadoopGateway = configuration.getClusterConf()
            .service(HadoopClusterConfiguration.HADOOP)
            .role(HadoopServiceDescriptor.GATEWAY)
            .instance(0)

        ServiceDescriptor hadoopServiceDescriptor = hadoopGateway.getServiceDescriptor()

        File baseDir = hadoopGateway.getBaseDir()
        File homeDir = new File(baseDir, hadoopServiceDescriptor.homeDirName(hadoopGateway))
        File confDir = new File(homeDir, hadoopServiceDescriptor.confDirName(hadoopGateway))

        // HADOOP_CONF_DIR -> ...../etc/hadoop/
        env.put('HADOOP_CONF_DIR', confDir.toString())
    }

    @Override
    void configureSetupTasks(InstanceConfiguration configuration, SetupTaskFactory taskFactory) {

    }

    @Override
    Map<String, Object[]> defaultSetupCommands(InstanceConfiguration configuration) {
        return [:]
    }
}