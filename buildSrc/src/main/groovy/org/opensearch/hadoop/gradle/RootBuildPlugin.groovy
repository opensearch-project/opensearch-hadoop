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

package org.opensearch.hadoop.gradle

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.bundling.Zip

/**
 * Plugin for configuring any settings/tasks on the root project that are needed for building the
 * master distribution (all integrations combined).
 * <p>
 * Note: This is only for defining settings/tasks that have a root/subproject relationship that have to
 * do with packaging. Root project tasks or configs that are subject to ALL projects (not just the ones
 * that are shipped in the master project archive) should be configured in {@link BuildPlugin}.
 *
 * @see IntegrationBuildPlugin
 */
class RootBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project target) {
        // Ensure that we're only every applying this to the root project
        if (target != target.rootProject) {
            throw new GradleException("Cannot apply root build plugin to non-root project [${target.name}].")
        }

        configurePlugins(target)
        configureProjectJar(target)
        configureProjectZip(target)
    }

    /**
     * Configure plugins required by the root project.
     * @param project to be configured
     */
    private static void configurePlugins(Project project) {
        // Common BuildPlugin should be configured first.
        project.getPluginManager().apply(BuildPlugin.class)
    }

    /**
     * Configure the master jar distribution process.
     * @param project to be configured
     */
    private static def configureProjectJar(Project project) {
        // Each integration will be copying it's entire jar contents into this master jar.
        // There will be lots of duplicates since they all package up the core code inside of them.
        Jar jar = project.getTasks().getByName('jar') as Jar
        jar.setDuplicatesStrategy(DuplicatesStrategy.EXCLUDE)

        if (project.logger.isDebugEnabled()) {
            jar.doLast {
                jar.getInputs().getFiles().each { project.logger.debug(":jar - Adding: $it") }
            }
        }
    }

    /**
     * Create a task that zips up all sub-project jars together.
     * Each subproject will register its own artifacts with this task for them to be published.
     * @param project to be configured
     */
    private static void configureProjectZip(Project project) {
        Zip distZip = project.getTasks().create('distZip', Zip.class)
        distZip.dependsOn(project.getTasks().getByName('pack'))
        distZip.setGroup('Distribution')
        distZip.setDescription("Builds -${distZip.archiveClassifier.get()} archive, containing all jars and docs, suitable for download page.")

        Task distribution = project.getTasks().getByName('distribution')
        distribution.dependsOn(distZip)

        // Location of the zip dir
        project.rootProject.ext.folderName = "${distZip.archiveBaseName.get()}" + "-" + "${project.version}"

        // Copy root directory files to zip
        distZip.from(project.rootDir) { CopySpec spec ->
            spec.include("README.md")
            spec.include("LICENSE.txt")
            spec.include("NOTICE.txt")
            spec.into("${project.rootProject.ext.folderName}")
        }

        // Copy master jar, sourceJar, and javadocJar to zip
        project.afterEvaluate {
            // Do not copy the hadoop testing jar
            project.getTasks().withType(Jar.class) { Jar jarTask ->
                distZip.from(jarTask.archiveFile) { CopySpec spec ->
                    spec.into("${project.rootProject.ext.folderName}/dist")
                }
            }
        }

        // Log dist artifacts
        distZip.doLast {
            distZip.getInputs().getFiles().each { project.logger.info(":distZip - Adding: $it")}
        }
    }
}