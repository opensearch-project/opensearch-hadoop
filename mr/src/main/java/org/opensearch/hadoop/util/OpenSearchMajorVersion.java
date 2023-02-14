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
package org.opensearch.hadoop.util;

import java.io.Serializable;

import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException;

/**
 * OpenSearch major version information, useful to check client's query compatibility with the Rest API.
 */
public class OpenSearchMajorVersion implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final OpenSearchMajorVersion V_1_X = new OpenSearchMajorVersion((byte) 1, "1.x");
    public static final OpenSearchMajorVersion LATEST = V_1_X;

    public final byte major;
    private final String version;

    private OpenSearchMajorVersion(byte major, String version) {
        this.major = major;
        this.version = version;
    }

    public boolean after(OpenSearchMajorVersion version) {
        return version.major < major;
    }

    public boolean on(OpenSearchMajorVersion version) {
        return version.major == major;
    }

    public boolean notOn(OpenSearchMajorVersion version) {
        return !on(version);
    }

    public boolean onOrAfter(OpenSearchMajorVersion version) {
        return version.major <= major;
    }

    public boolean before(OpenSearchMajorVersion version) {
        return version.major > major;
    }

    public boolean onOrBefore(OpenSearchMajorVersion version) {
        return version.major >= major;
    }

    public static OpenSearchMajorVersion parse(String version) {
        if (version.startsWith("1.")) {
            return new OpenSearchMajorVersion((byte) 1, version);
        }
        throw new OpenSearchHadoopIllegalArgumentException("Unsupported/Unknown OpenSearch version [" + version + "]." +
                "Highest supported version is [" + LATEST.version + "]. You may need to upgrade OpenSearch-Hadoop.");
    }

    public int parseMinorVersion(String versionString) {
        String majorPrefix = "" + major + ".";
        if (versionString.startsWith(majorPrefix) == false) {
            throw new OpenSearchHadoopIllegalArgumentException("Invalid version string for major version; " +
                    "Received [" + versionString + "] for major version [" + version + "]");
        }
        String minorRemainder = versionString.substring(majorPrefix.length());
        int dot = minorRemainder.indexOf('.');
        if (dot < 1) {
            throw new OpenSearchHadoopIllegalArgumentException("Could not parse OpenSearch minor version [" +
                    versionString + "]. Invalid version format.");
        }
        String rawMinorVersion = minorRemainder.substring(0, dot);
        try {
            return Integer.parseInt(rawMinorVersion);
        } catch (NumberFormatException e) {
            throw new OpenSearchHadoopIllegalArgumentException("Could not parse OpenSearch minor version [" +
                    versionString + "]. Non-numeric minor version [" + rawMinorVersion + "].", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OpenSearchMajorVersion version = (OpenSearchMajorVersion) o;

        return major == version.major &&
                this.version.equals(version.version);
    }

    @Override
    public int hashCode() {
        return major;
    }

    @Override
    public String toString() {
        return version;
    }
}