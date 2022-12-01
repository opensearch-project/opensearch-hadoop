A = LOAD 'qa_kerberos_pig_data' USING org.opensearch.hadoop.pig.OpenSearchStorage(
    'opensearch.security.authentication = kerberos',
    'opensearch.net.spnego.auth.opensearch.principal = HTTP/build.ci.opensearch.org@BUILD.CI.OPENSEARCH.ORG'
);

STORE A INTO '/data/output/pig' USING PigStorage('\t');