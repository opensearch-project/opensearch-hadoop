A = LOAD '/data/artists' USING PigStorage('\t') AS (number: chararray, name: chararray, uri: chararray, picture: chararray, timestamp: chararray, tag: chararray);

STORE A INTO 'qa_kerberos_pig_data' USING org.opensearch.pig.hadoop.OpenSearchStorage(
    'opensearch.security.authentication = kerberos',
    'opensearch.net.spnego.auth.opensearch.principal = HTTP/build.ci.opensearch.org@BUILD.CI.OPENSEARCH.ORG'
);