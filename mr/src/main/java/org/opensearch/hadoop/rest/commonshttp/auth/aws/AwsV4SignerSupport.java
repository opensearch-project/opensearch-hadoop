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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.ZoneOffset;

public class AwsV4SignerSupport {
    private final Settings settings;
    private final String httpInfo;
    private final AwsCredentials credentials;
    private Date overriddenSigningDate;

    public AwsV4SignerSupport(Settings settings, String httpInfo, AwsCredentials credentials) {
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

        SdkHttpFullRequest.Builder rb = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.valueOf(request.method().name()))
                .uri(java.net.URI.create(httpInfo))
                .encodedPath(request.path().toString());
        
        CharSequence params = request.params();
        if (params != null && params.length() > 0) {
            Splitter.on('&').trimResults().omitEmptyStrings()
                    .split(params.toString())
                    .forEach(p -> {
                        try {
                            int idx = p.indexOf('=');
                            String k = idx > 0 ? URLDecoder.decode(p.substring(0, idx), StandardCharsets.UTF_8.name()) : p;
                            String v = idx > 0 ? URLDecoder.decode(p.substring(idx + 1), StandardCharsets.UTF_8.name()) : "";
                            rb.putRawQueryParameter(k, v);
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException("UTF-8 encoding not supported", e);
                        }
                    });
        }

        for (Header h : http.getRequestHeaders()) {
            if (!h.getName().equalsIgnoreCase("user-agent")) {
                rb.appendHeader(h.getName(), h.getValue());
            }
        }
        rb.appendHeader("x-amz-content-sha256", "required");

        rb.contentStreamProvider(() -> request.body() != null
                ? request.body().toInputStream()
                : new ByteArrayInputStream(new byte[0]));

        SdkHttpFullRequest unsigned = rb.build();

        Aws4Signer signer = Aws4Signer.create();

        Aws4SignerParams.Builder paramsBuilder = Aws4SignerParams.builder()
                .awsCredentials(credentials)
                .signingRegion(Region.of(awsRegion))
                .signingName(awsServiceName);

        if (overriddenSigningDate != null) {
            paramsBuilder.signingClockOverride(
                    Clock.fixed(overriddenSigningDate.toInstant(), ZoneOffset.UTC));
        }

        SdkHttpFullRequest signed = signer.sign(unsigned, paramsBuilder.build());

        signed.headers().forEach((k, v) -> http.setRequestHeader(k, v.get(0)));
    }
}
