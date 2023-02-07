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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.thirdparty.google.common.base.Optional;
import org.opensearch.hadoop.thirdparty.google.common.base.Splitter;
import org.opensearch.hadoop.thirdparty.google.common.base.Strings;
import org.opensearch.hadoop.thirdparty.google.common.base.Supplier;
import org.opensearch.hadoop.thirdparty.google.common.collect.ImmutableListMultimap;
import org.opensearch.hadoop.thirdparty.google.common.collect.Multimap;
import org.opensearch.hadoop.thirdparty.google.common.collect.ImmutableMap;
import org.opensearch.hadoop.thirdparty.google.common.collect.ImmutableList;
import org.opensearch.hadoop.thirdparty.google.common.base.Joiner;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.regions.Regions;
import com.amazonaws.util.SdkHttpUtils;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import org.opensearch.hadoop.util.ByteSequence;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;

public class AwsV4Signer {

    private static Log log = LogFactory.getLog(AwsV4Signer.class);

    private final Settings settings;
    private final String httpInfo;
    private final AWSCredentials credentials;

    private static final Joiner AMPERSAND_JOINER = Joiner.on('&');

    public AwsV4Signer(Settings settings, String httpInfo) {
        this(settings, httpInfo, null);
    }

    public AwsV4Signer(Settings settings, String httpInfo, AWSCredentials credentials) {
        this.settings = settings;
        this.httpInfo = httpInfo;
        this.credentials = credentials;
    }

    private String queryParamsString(Multimap<String, String> queryParams) {
        final ImmutableList.Builder<String> result = ImmutableList.builder();
        for (Map.Entry<String, Collection<String>> param : new TreeMap<>(queryParams.asMap()).entrySet()) {
            for (String value : param.getValue()) {
                result.add(SdkHttpUtils.urlEncode(param.getKey(), false) + '=' + SdkHttpUtils.urlEncode(value, false));
            }
        }

        return AMPERSAND_JOINER.join(result.build());
    }

    public void sign(Request request, HttpMethod http)
            throws UnsupportedEncodingException {
        String awsRegion = settings.getAwsSigV4Region();
        String awsServiceName = settings.getAwsSigV4ServiceName();

        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setRegionName(awsRegion);
        aws4Signer.setServiceName(awsServiceName);

        DefaultRequest<Void> req = new DefaultRequest<>(awsServiceName);
        req.setHttpMethod(HttpMethodName.valueOf(request.method().name()));

        StringBuilder url = new StringBuilder();
        url.append(httpInfo);

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
                    queryParams.put(key, Arrays.asList(value));
                } else {
                    queryParams.put(pair, Arrays.asList(""));
                }
            }
        }

        req.setParameters(queryParams);

        try {
            req.setEndpoint(new java.net.URI(url.toString()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid request URI: " + request.uri().toString());
        }

        if (request.body() != null) {
            req.setContent(request.body().toInputStream());
        }

        req.addHeader("x-amz-content-sha256", "required");

        aws4Signer.sign(req, credentials);

        for (Map.Entry<String, String> entry : req.getHeaders().entrySet()) {
            http.setRequestHeader(entry.getKey(), entry.getValue());
        }
    }
}
