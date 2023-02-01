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

import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.auth.signer.Aws4Signer;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.regions.Region;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.http.SdkHttpFullRequest;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.http.SdkHttpClient;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.http.SdkHttpMethod;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.http.SdkHttpResponse;
import org.opensearch.hadoop.thirdparty.amazon.awssdk.utils.http.SdkHttpUtils;
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

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.Collection;


public class AwsV4Signer {
    private final Settings settings;
    private final String httpInfo;

    private static final Joiner AMPERSAND_JOINER = Joiner.on('&');

    public AwsV4Signer(Settings settings, String httpInfo) {
        this.settings = settings;
        this.httpInfo = httpInfo;
    }

    private String queryParamsString(Multimap<String, String> queryParams) {
        final ImmutableList.Builder<String> result = ImmutableList.builder();
        for (Map.Entry<String, Collection<String>> param : new TreeMap<>(queryParams.asMap()).entrySet()) {
            for (String value : param.getValue()) {
                result.add(SdkHttpUtils.urlEncode(param.getKey()) + '=' + SdkHttpUtils.urlEncode(value));
            }
        }

        return '?' + AMPERSAND_JOINER.join(result.build());
    }

    public void sign(Request request, HttpMethod http, byte[] bodyBytes)
            throws UnsupportedEncodingException {
        String awsRegion = settings.getAwsSigV4Region();
        String awsServiceName = settings.getAwsSigV4ServiceName();

        Region signingRegion = Region.of(awsRegion);

        final AwsCredentialsProvider credentials = DefaultCredentialsProvider.create();

        Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentials.resolveCredentials())
                .signingName(awsServiceName)
                .signingRegion(signingRegion)
                .build();

        SdkHttpFullRequest.Builder req = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.fromValue(request.method().name()));

        StringBuilder url = new StringBuilder();
        url.append(httpInfo);
        String path = request.path().toString();
        if (!path.startsWith("/")) {
            url.append('/');
        }
        url.append(path);

        Splitter queryStringSplitter = Splitter.on('&').trimResults().omitEmptyStrings();
        final Iterable<String> rawParams = request.params() != null ? queryStringSplitter.split(request.params())
                : Collections.emptyList();

        final ImmutableListMultimap.Builder<String, String> queryParams = ImmutableListMultimap.builder();

        for (String rawParam : rawParams) {
            if (!Strings.isNullOrEmpty(rawParam)) {
                final String pair = URLDecoder.decode(rawParam, StandardCharsets.UTF_8.name());
                final int index = pair.indexOf('=');
                if (index > 0) {
                    final String key = pair.substring(0, index);
                    final String value = pair.substring(index + 1);
                    queryParams.put(key, value);
                } else {
                    queryParams.put(pair, "");
                }
            }
        }

        String params = queryParamsString(queryParams.build());

        url.append(params);

        try {
            req.uri(new java.net.URI(url.toString()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid request URI: " + request.uri().toString());
        }

        if (request.body() != null) {
            req.contentStreamProvider(() -> new ByteArrayInputStream(bodyBytes));
        }
        req.putHeader("x-amz-content-sha256", "required");

        SdkHttpFullRequest signedRequest = Aws4Signer.create().sign(req.build(), signerParams);

        final ImmutableMap.Builder<String, String> signerHeaders = ImmutableMap.builder();

        for (Map.Entry<String, List<String>> entry : signedRequest.headers().entrySet()) {
            http.setRequestHeader(entry.getKey(), entry.getValue().get(0));
        }
    }
}
