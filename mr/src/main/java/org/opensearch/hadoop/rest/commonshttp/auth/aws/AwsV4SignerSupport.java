/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.hadoop.rest.commonshttp.auth.aws;

import java.io.ByteArrayInputStream;
import java.util.Date;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Header;
import org.opensearch.hadoop.thirdparty.google.common.base.Splitter;
import org.opensearch.hadoop.thirdparty.google.common.base.Strings;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsV4SignerSupport {
    private final Settings settings;
    private final String httpInfo;
    private final AWSCredentials credentials;
    private Date overriddenSigningDate;

    public AwsV4SignerSupport(Settings settings, String httpInfo, AWSCredentials credentials) {
        this.settings = settings;
        this.httpInfo = httpInfo;
        this.credentials = credentials;
    }

    /**
     * Overrides the signing date for the request for testing purposes.
     * @param signingDate The fixed signing date to use.
     */
    void overrideSigningDate(Date signingDate) {
        this.overriddenSigningDate = signingDate;
    }

    public void sign(Request request, HttpMethod http)
            throws UnsupportedEncodingException {
        String awsRegion = settings.getAwsSigV4Region();
        String awsServiceName = settings.getAwsSigV4ServiceName();

        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setRegionName(awsRegion);
        aws4Signer.setServiceName(awsServiceName);

        if (overriddenSigningDate != null) {
            aws4Signer.setOverrideDate(overriddenSigningDate);
        }

        DefaultRequest<Void> req = new DefaultRequest<>(awsServiceName);
        req.setHttpMethod(HttpMethodName.valueOf(request.method().name()));

        String path = request.path().toString();
        req.setResourcePath(path);

        Splitter queryStringSplitter = Splitter.on('&').trimResults().omitEmptyStrings();
        final Iterable<String> rawParams = request.params() != null ? queryStringSplitter.split(request.params())
                : Collections.emptyList();

        Map<String, List<String>> queryParams = new HashMap<>();

        for (String rawParam : rawParams) {
            if (!Strings.isNullOrEmpty(rawParam)) {
                final String pair = URLDecoder.decode(rawParam, StandardCharsets.UTF_8.name());
                final int index = pair.indexOf('=');
                if (index > 0) {
                    final String key = pair.substring(0, index);
                    final String value = pair.substring(index + 1);
                    queryParams.put(key, Collections.singletonList(value));
                } else {
                    queryParams.put(pair, Collections.singletonList(""));
                }
            }
        }

        req.setParameters(queryParams);

        try {
            req.setEndpoint(new java.net.URI(httpInfo));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid request URI: " + httpInfo);
        }

        // Explicitly provide an empty body stream to avoid the signer utilizing the query params as the body content
        // See: https://github.com/opensearch-project/opensearch-hadoop/pull/443
        req.setContent(request.body() != null ? request.body().toInputStream() : new ByteArrayInputStream(new byte[0]));

        for (Header header : http.getRequestHeaders()) {
            if (header.getName().equalsIgnoreCase("user-agent")) continue;

            req.addHeader(header.getName(), header.getValue());
        }

        req.addHeader("x-amz-content-sha256", "required");

        aws4Signer.sign(req, credentials);

        for (Map.Entry<String, String> entry : req.getHeaders().entrySet()) {
            http.setRequestHeader(entry.getKey(), entry.getValue());
        }
    }
}
