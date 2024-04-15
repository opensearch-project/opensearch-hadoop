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

import java.time.Instant;
import java.util.Date;
import org.junit.Test;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.HeaderProcessor;
import org.opensearch.hadoop.rest.SimpleRequest;
import org.opensearch.hadoop.rest.Request.Method;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.methods.PostMethod;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.methods.PutMethod;
import org.opensearch.hadoop.util.BytesArray;
import org.opensearch.hadoop.util.TestSettings;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class AwsSigV4SignerTest {
    private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static Settings getTestSettings(String serviceName) {
        Settings settings = new TestSettings("awsSigV4Signer");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_NODES,
                             "https://aaabbbcccddd111222333.ap-southeast-2." + serviceName + ".amazonaws.com:443");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_NET_HTTP_HEADER_USER_AGENT, "aribtrary-user-agent");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_ENABLED, "true");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_REGION, "ap-southeast-2");
        if (serviceName != null && !serviceName.equals("es")) {
            settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_SERVICE_NAME, serviceName);
        }
        return settings;
    }

    private static AwsV4SignerSupport getTestSigner(Settings settings) {
        AWSCredentials credentials = new BasicAWSCredentials("test-access-key", "test-secret-key");
        AwsV4SignerSupport signer = new AwsV4SignerSupport(settings, settings.getNodes(), credentials);
        signer.overrideSigningDate(Date.from(Instant.ofEpochSecond(1673626117))); // 2023-01-13 16:08:37 +0000
        return signer;
    }

    public void testSigV4PutIndex(String serviceName, String expectedSignature)
            throws Exception {
        Settings settings = getTestSettings(serviceName);

        SimpleRequest req = new SimpleRequest(
                Method.PUT,
                null,
                "sample-index1",
                new BytesArray("{"
                    + "\"aliases\":{\"sample-alias1\":{}},"
                    + "\"mappings\":{"
                        + "\"properties\":{"
                            + "\"age\":{\"type\":\"integer\"}"
                        + "}"
                    + "},"
                    + "\"settings\":{"
                        + "\"index.number_of_replicas\":1,"
                        + "\"index.number_of_shards\":2"
                    + "}"
                + "}"));
        HttpMethod http = new PutMethod();

        new HeaderProcessor(settings).applyTo(http);

        getTestSigner(settings).sign(req, http);

        String xAmzDate = http.getRequestHeader("X-Amz-Date").getValue();
        String xAmzContentSha256 = http.getRequestHeader("X-Amz-Content-SHA256").getValue();
        String authorization = http.getRequestHeader("Authorization").getValue();

        assertThat(xAmzDate, is("20230113T160837Z"));
        assertThat(xAmzContentSha256, is("4c770eaed349122a28302ff73d34437cad600acda5a9dd373efc7da2910f8564"));
        assertThat(
                authorization,
                is(
                        "AWS4-HMAC-SHA256 "
                        + "Credential=test-access-key/20230113/ap-southeast-2/" + serviceName + "/aws4_request, "
                        + "SignedHeaders=accept;content-type;host;x-amz-content-sha256;x-amz-date, "
                        + "Signature=" + expectedSignature
                ));
    }

    @Test
    public void testSigV4PutIndexWithEsServiceName() throws Exception {
        testSigV4PutIndex("es", "10c9be415f4b9f15b12abbb16bd3e3730b2e6c76e0cf40db75d08a44ed04a3a1");
    }

    /**
     * opensearch-hadoop DOES NOT currently function against AOSS due to missing APIs in the service,
     * however ensure the SigV4 signing routine correctly signs requests.
     */
    @Test
    public void testSigV4PutIndexWithAossServiceName() throws Exception {
        testSigV4PutIndex("aoss", "34903aef90423aa7dd60575d3d45316c6ef2d57bbe564a152b41bf8f5917abf6");
    }

    @Test
    public void testSigV4PutIndexWithArbitraryServiceName() throws Exception {
        testSigV4PutIndex("arbitrary", "156e65c504ea2b2722a481b7515062e7692d27217b477828854e715f507e6a36");
    }

    /**
     * Reproducing <a href="https://github.com/opensearch-project/opensearch-hadoop/pull/350#issuecomment-2039290838">#350 - comment</a>
     */
    @Test
    public void testSigV4PostSearchWithQueryParamsAndNoBody() throws Exception {
        Settings settings = getTestSettings("es");

        SimpleRequest req = new SimpleRequest(
                Method.POST,
                null,
                "sample-index1/_search",
                "scroll=10m&_source=false&size=500&sort=_doc",
                null);
        HttpMethod http = new PostMethod();

        new HeaderProcessor(settings).applyTo(http);

        getTestSigner(settings).sign(req, http);

        String xAmzDate = http.getRequestHeader("X-Amz-Date").getValue();
        String xAmzContentSha256 = http.getRequestHeader("X-Amz-Content-SHA256").getValue();
        String authorization = http.getRequestHeader("Authorization").getValue();

        assertThat(xAmzDate, is("20230113T160837Z"));
        assertThat(xAmzContentSha256, is(EMPTY_SHA256));
        assertThat(
                authorization,
                is(
                        "AWS4-HMAC-SHA256 "
                        + "Credential=test-access-key/20230113/ap-southeast-2/es/aws4_request, "
                        + "SignedHeaders=accept;content-type;host;x-amz-content-sha256;x-amz-date, "
                        + "Signature="
                ));
    }
}
