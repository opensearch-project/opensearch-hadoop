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

package org.opensearch.hadoop.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.opensearch.hadoop.OpenSearchHadoopException;
import org.opensearch.hadoop.util.OpenSearchMajorVersion;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ErrorExtractorTest {
	@Test
	public void extractErrorWithCause() {
		final Map<String, String> nestedCause = ImmutableMap.<String, String>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Failed to parse value [not_analyzed] as only [true] or [false] are allowed.")
				.build();
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.put("caused_by", nestedCause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor();
		
		final OpenSearchHadoopException ex = extractor.extractErrorWithCause(cause);
		checkException(ex, cause);
		
	}
	@Test
	public void extractErrorV5() {
		final Map<String, String> nestedCause = ImmutableMap.<String, String>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Failed to parse value [not_analyzed] as only [true] or [false] are allowed.")
				.build();
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.put("caused_by", nestedCause)
				.build();
		
		final Map<String, Object> error = ImmutableMap.<String, Object>builder()
				.put("error", cause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor();
		
		final OpenSearchHadoopException ex = extractor.extractError(error);
		checkException(ex, cause);
		
	}
	@Test
	public void extractErrorV2() {
		final Map<String, Object> cause = ImmutableMap.<String, Object>builder()
				.put("type", "illegal_argument_exception")
				.put("reason", "Could not convert [version.index] to boolean")
				.build();
		
		final Map<String, Object> error = ImmutableMap.<String, Object>builder()
				.put("error", cause)
				.build();
		
		final ErrorExtractor extractor = new ErrorExtractor();
		
		final OpenSearchHadoopException ex = extractor.extractError(error);
		checkException(ex, cause);
		
	}
	
	@SuppressWarnings("unchecked")
	protected void checkException(Throwable ex, Map<String, ?> json) {
		assertNotNull(ex);
		assertTrue(OpenSearchHadoopRemoteException.class.isAssignableFrom(ex.getClass()));
		
		final OpenSearchHadoopRemoteException exRemote = (OpenSearchHadoopRemoteException)ex;
		
		assertEquals(json.get("type"), exRemote.getType());
		assertEquals(json.get("reason"), exRemote.getMessage());
		
		if(json.containsKey("caused_by")) {
			assertNotNull(exRemote.getCause());
			checkException(exRemote.getCause(), (Map<String, ?>) json.get("caused_by"));
		}
	}
}