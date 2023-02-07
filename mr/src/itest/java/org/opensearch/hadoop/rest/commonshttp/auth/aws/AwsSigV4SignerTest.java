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

import org.junit.Test;
import org.opensearch.hadoop.cfg.ConfigurationOptions;
import org.opensearch.hadoop.cfg.Settings;
import org.opensearch.hadoop.rest.Request;
import org.opensearch.hadoop.rest.SimpleRequest;
import org.opensearch.hadoop.rest.Request.Method;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.methods.GetMethod;
import org.opensearch.hadoop.util.TestSettings;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;

import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;
import org.opensearch.hadoop.thirdparty.apache.commons.httpclient.Header;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;

public class AwsSigV4SignerTest {

    @Test
    public void testSigV4() throws Exception
    {
        String httpInfo = "http://localhost";
        SimpleRequest simpleRequest = new SimpleRequest(Method.GET, httpInfo, "/");
        HttpMethod httpMethod = new GetMethod();

        Settings settings = new TestSettings("rest/awsSigV4Signer");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_ENABLED, "true");

        final String awsAccessKeyId = "foo";
        final String awsSecretAccessKey = "bar";
        final AWSCredentials reqCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);

        AwsV4Signer awsV4Signer = new AwsV4Signer(settings, httpInfo, reqCredentials);
        awsV4Signer.sign(simpleRequest, httpMethod);

        Header xAmzDateHeader = httpMethod.getRequestHeader("X-Amz-Date");
        Header authorization = httpMethod.getRequestHeader("Authorization");
        Header xAmzContentSha256 = httpMethod.getRequestHeader("X-Amz-Content-SHA256");

        assertThat(xAmzDateHeader, notNullValue());
        assertThat(authorization, notNullValue());
        assertThat(xAmzContentSha256, notNullValue());
    }

    @Test
    public void testSigV4WithCustomRegionAndService() throws Exception {
        String httpInfo = "http://localhost";
        SimpleRequest simpleRequest = new SimpleRequest(Method.GET, httpInfo, "/");
        HttpMethod httpMethod = new GetMethod();

        Settings settings = new TestSettings("rest/awsSigV4Signer");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_ENABLED, "true");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_REGION, "us-west-2");
        settings.setProperty(ConfigurationOptions.OPENSEARCH_AWS_SIGV4_SERVICE_NAME, "aoss");
        
        final String awsAccessKeyId = "foo";
        final String awsSecretAccessKey = "bar";
        final AWSCredentials reqCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);

        AwsV4Signer awsV4Signer = new AwsV4Signer(settings, httpInfo, reqCredentials);
        awsV4Signer.sign(simpleRequest, httpMethod);

        Header xAmzDateHeader = httpMethod.getRequestHeader("X-Amz-Date");
        Header authorization = httpMethod.getRequestHeader("Authorization");
        Header xAmzContentSha256 = httpMethod.getRequestHeader("X-Amz-Content-SHA256");

        assertThat(xAmzDateHeader, notNullValue());
        assertThat(authorization, notNullValue());
        assertThat(xAmzContentSha256, notNullValue());
    }
    
}
