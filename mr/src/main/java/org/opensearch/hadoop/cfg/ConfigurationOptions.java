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
package org.opensearch.hadoop.cfg;

import org.opensearch.hadoop.serialization.field.DateIndexFormatter;
import org.opensearch.hadoop.serialization.field.DefaultIndexExtractor;
import org.opensearch.hadoop.serialization.field.DefaultParamsExtractor;

/**
 * Class providing the various Configuration parameters used by the OpenSearch Hadoop integration.
 */
public interface ConfigurationOptions {

    /** OpenSearch host **/
    String OPENSEARCH_NODES = "opensearch.nodes";
    String OPENSEARCH_NODES_DEFAULT = "localhost";

    String OPENSEARCH_NODES_DISCOVERY = "opensearch.nodes.discovery";
    String OPENSEARCH_NODES_DISCOVERY_DEFAULT = "true";

    /** OpenSearch port **/
    String OPENSEARCH_PORT = "opensearch.port";
    String OPENSEARCH_PORT_DEFAULT = "9200";

    /** OpenSearch prefix **/
    String OPENSEARCH_NODES_PATH_PREFIX = "opensearch.nodes.path.prefix";
    String OPENSEARCH_NODES_PATH_PREFIX_DEFAULT = "";

    /** OpenSearch index */
    String OPENSEARCH_RESOURCE = "opensearch.resource";
    String OPENSEARCH_RESOURCE_READ = "opensearch.resource.read";
    String OPENSEARCH_RESOURCE_WRITE = "opensearch.resource.write";

    String OPENSEARCH_QUERY = "opensearch.query";

    /** Clients only */
    String OPENSEARCH_NODES_CLIENT_ONLY = "opensearch.nodes.client.only";
    String OPENSEARCH_NODES_CLIENT_ONLY_DEFAULT = "false";

    /** Data only */
    String OPENSEARCH_NODES_DATA_ONLY = "opensearch.nodes.data.only";
    String OPENSEARCH_NODES_DATA_ONLY_DEFAULT = "true";

    /** Ingest only */
    String OPENSEARCH_NODES_INGEST_ONLY = "opensearch.nodes.ingest.only";
    String OPENSEARCH_NODES_INGEST_ONLY_DEFAULT = "false";

    /** WAN only */
    String OPENSEARCH_NODES_WAN_ONLY = "opensearch.nodes.wan.only";
    String OPENSEARCH_NODES_WAN_ONLY_DEFAULT = "false";

    String OPENSEARCH_NODES_RESOLVE_HOST_NAME = "opensearch.nodes.resolve.hostname";

    /** Secure Settings Keystore */
    String OPENSEARCH_KEYSTORE_LOCATION = "opensearch.keystore.location";

    /** OpenSearch batch size given in bytes */
    String OPENSEARCH_BATCH_SIZE_BYTES = "opensearch.batch.size.bytes";
    String OPENSEARCH_BATCH_SIZE_BYTES_DEFAULT = "1mb";

    /** OpenSearch batch size given in entries */
    String OPENSEARCH_BATCH_SIZE_ENTRIES = "opensearch.batch.size.entries";
    String OPENSEARCH_BATCH_SIZE_ENTRIES_DEFAULT = "1000";

    /** OpenSearch disable auto-flush on batch overflow */
    String OPENSEARCH_BATCH_FLUSH_MANUAL = "opensearch.batch.flush.manual";
    String OPENSEARCH_BATCH_FLUSH_MANUAL_DEFAULT = "false";

    /** Whether to trigger an index refresh after doing batch writing */
    String OPENSEARCH_BATCH_WRITE_REFRESH = "opensearch.batch.write.refresh";
    String OPENSEARCH_BATCH_WRITE_REFRESH_DEFAULT = "true";

    /** HTTP bulk retries **/
    String OPENSEARCH_BATCH_WRITE_RETRY_COUNT = "opensearch.batch.write.retry.count";
    String OPENSEARCH_BATCH_WRITE_RETRY_COUNT_DEFAULT = "3";

    String OPENSEARCH_BATCH_WRITE_RETRY_LIMIT = "opensearch.batch.write.retry.limit";
    String OPENSEARCH_BATCH_WRITE_RETRY_LIMIT_DEFAULT = "50";

    String OPENSEARCH_BATCH_WRITE_RETRY_WAIT = "opensearch.batch.write.retry.wait";
    String OPENSEARCH_BATCH_WRITE_RETRY_WAIT_DEFAULT = "10s";

    String OPENSEARCH_BATCH_WRITE_RETRY_POLICY = "opensearch.batch.write.retry.policy";
    String OPENSEARCH_BATCH_WRITE_RETRY_POLICY_NONE = "none";
    String OPENSEARCH_BATCH_WRITE_RETRY_POLICY_SIMPLE = "simple";
    String OPENSEARCH_BATCH_WRITE_RETRY_POLICY_DEFAULT = OPENSEARCH_BATCH_WRITE_RETRY_POLICY_SIMPLE;

    /** HTTP connection timeout */
    String OPENSEARCH_HTTP_TIMEOUT = "opensearch.http.timeout";
    String OPENSEARCH_HTTP_TIMEOUT_DEFAULT = "1m";

    String OPENSEARCH_HTTP_RETRIES = "opensearch.http.retries";
    String OPENSEARCH_HTTP_RETRIES_DEFAULT = "3";

    /** Scroll keep-alive */
    String OPENSEARCH_SCROLL_KEEPALIVE = "opensearch.scroll.keepalive";
    String OPENSEARCH_SCROLL_KEEPALIVE_DEFAULT = "5m";

    /** Scroll size */
    String OPENSEARCH_SCROLL_SIZE = "opensearch.scroll.size";
    String OPENSEARCH_SCROLL_SIZE_DEFAULT = "1000";

    /** Scroll limit */
    String OPENSEARCH_SCROLL_LIMIT = "opensearch.scroll.limit";
    String OPENSEARCH_SCROLL_LIMIT_DEFAULT = "-1";

    /** Scroll fields */

    String OPENSEARCH_HEART_BEAT_LEAD = "opensearch.action.heart.beat.lead";
    String OPENSEARCH_HEART_BEAT_LEAD_DEFAULT = "15s";

    /** Serialization settings */

    /** Value writer - setup automatically; can be overridden for custom types */
    String OPENSEARCH_SERIALIZATION_WRITER_VALUE_CLASS = "opensearch.ser.writer.value.class";

    /** JSON/Bytes writer - setup automatically; can be overridden for custom types */
    String OPENSEARCH_SERIALIZATION_WRITER_BYTES_CLASS = "opensearch.ser.writer.bytes.class";

    /** Value reader - setup automatically; can be overridden for custom types */
    String OPENSEARCH_SERIALIZATION_READER_VALUE_CLASS = "opensearch.ser.reader.value.class";

    /** Input options **/
    String OPENSEARCH_MAX_DOCS_PER_PARTITION = "opensearch.input.max.docs.per.partition";

    String OPENSEARCH_INPUT_JSON = "opensearch.input.json";
    String OPENSEARCH_INPUT_JSON_DEFAULT = "no";

    /** Index settings */
    String OPENSEARCH_INDEX_AUTO_CREATE = "opensearch.index.auto.create";
    String OPENSEARCH_INDEX_AUTO_CREATE_DEFAULT = "yes";

    String OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY = "opensearch.index.read.missing.as.empty";
    String OPENSEARCH_INDEX_READ_MISSING_AS_EMPTY_DEFAULT = "false";

    String OPENSEARCH_INDEX_READ_ALLOW_RED_STATUS = "opensearch.index.read.allow.red.status";
    String OPENSEARCH_INDEX_READ_ALLOW_RED_STATUS_DEFAULT = "false";

    /** OpenSearch shard search preference */
    String OPENSEARCH_READ_SHARD_PREFERENCE = "opensearch.read.shard.preference";
    String OPENSEARCH_READ_SHARD_PREFERENCE_DEFAULT = "";

    /** Mapping types */
    String OPENSEARCH_MAPPING_DEFAULT_EXTRACTOR_CLASS = "opensearch.mapping.default.extractor.class";
    
    String OPENSEARCH_MAPPING_METADATA_EXTRACTOR_CLASS = "opensearch.mapping.metadata.extractor.class";

    String OPENSEARCH_MAPPING_ID = "opensearch.mapping.id";
    String OPENSEARCH_MAPPING_ID_EXTRACTOR_CLASS = "opensearch.mapping.id.extractor.class";
    String OPENSEARCH_MAPPING_PARENT = "opensearch.mapping.parent";
    String OPENSEARCH_MAPPING_PARENT_EXTRACTOR_CLASS = "opensearch.mapping.parent.extractor.class";
    String OPENSEARCH_MAPPING_JOIN = "opensearch.mapping.join";
    String OPENSEARCH_MAPPING_JOIN_EXTRACTOR_CLASS = "opensearch.mapping.join.extractor.class";
    String OPENSEARCH_MAPPING_VERSION = "opensearch.mapping.version";
    String OPENSEARCH_MAPPING_VERSION_EXTRACTOR_CLASS = "opensearch.mapping.version.extractor.class";
    String OPENSEARCH_MAPPING_ROUTING = "opensearch.mapping.routing";
    String OPENSEARCH_MAPPING_ROUTING_EXTRACTOR_CLASS = "opensearch.mapping.routing.extractor.class";
    String OPENSEARCH_MAPPING_TTL = "opensearch.mapping.ttl";
    String OPENSEARCH_MAPPING_TTL_EXTRACTOR_CLASS = "opensearch.mapping.ttl.extractor.class";
    String OPENSEARCH_MAPPING_TIMESTAMP = "opensearch.mapping.timestamp";
    String OPENSEARCH_MAPPING_TIMESTAMP_EXTRACTOR_CLASS = "opensearch.mapping.timestamp.extractor.class";
    String OPENSEARCH_MAPPING_INDEX_EXTRACTOR_CLASS = "opensearch.mapping.index.extractor.class";
    String OPENSEARCH_MAPPING_DEFAULT_INDEX_EXTRACTOR_CLASS = DefaultIndexExtractor.class.getName();
    String OPENSEARCH_MAPPING_INDEX_FORMATTER_CLASS = "opensearch.mapping.index.formatter.class";
    String OPENSEARCH_MAPPING_DEFAULT_INDEX_FORMATTER_CLASS = DateIndexFormatter.class.getName();
    String OPENSEARCH_MAPPING_PARAMS_EXTRACTOR_CLASS = "opensearch.mapping.params.extractor.class";
    String OPENSEARCH_MAPPING_PARAMS_DEFAULT_EXTRACTOR_CLASS = DefaultParamsExtractor.class.getName();
    String OPENSEARCH_MAPPING_CONSTANT_AUTO_QUOTE = "opensearch.mapping.constant.auto.quote";
    String OPENSEARCH_MAPPING_CONSTANT_AUTO_QUOTE_DEFAULT = "true";

    String OPENSEARCH_MAPPING_DATE_RICH_OBJECT = "opensearch.mapping.date.rich";
    String OPENSEARCH_MAPPING_DATE_RICH_OBJECT_DEFAULT = "true";

    String OPENSEARCH_MAPPING_VERSION_TYPE = "opensearch.mapping.version.type";
    String OPENSEARCH_MAPPING_VERSION_TYPE_INTERNAL = "internal";
    String OPENSEARCH_MAPPING_VERSION_TYPE_EXTERNAL = "external";
    String OPENSEARCH_MAPPING_VERSION_TYPE_EXTERNAL_GT = "external_gt";
    String OPENSEARCH_MAPPING_VERSION_TYPE_EXTERNAL_GTE = "external_gte";
    String OPENSEARCH_MAPPING_VERSION_TYPE_FORCE = "force";

    String OPENSEARCH_MAPPING_INCLUDE = "opensearch.mapping.include";
    String OPENSEARCH_MAPPING_INCLUDE_DEFAULT = "";
    String OPENSEARCH_MAPPING_EXCLUDE = "opensearch.mapping.exclude";
    String OPENSEARCH_MAPPING_EXCLUDE_DEFAULT = "";

    /** Ingest Node **/
    String OPENSEARCH_INGEST_PIPELINE = "opensearch.ingest.pipeline";
    String OPENSEARCH_INGEST_PIPELINE_DEFAULT = "";

    /** Technology Specific **/
    String OPENSEARCH_SPARK_DATAFRAME_WRITE_NULL_VALUES = "opensearch.spark.dataframe.write.null";
    String OPENSEARCH_SPARK_DATAFRAME_WRITE_NULL_VALUES_DEFAULT = "false";

    /** Read settings */

    /** Field options **/
    String OPENSEARCH_READ_FIELD_EMPTY_AS_NULL = "opensearch.read.field.empty.as.null";
    String OPENSEARCH_READ_FIELD_EMPTY_AS_NULL_LEGACY = "opensearch.field.read.empty.as.null";
    String OPENSEARCH_READ_FIELD_EMPTY_AS_NULL_DEFAULT = "yes";

    String OPENSEARCH_READ_FIELD_VALIDATE_PRESENCE = "opensearch.read.field.validate.presence";
    String OPENSEARCH_READ_FIELD_VALIDATE_PRESENCE_LEGACY = "opensearch.field.read.validate.presence";
    String OPENSEARCH_READ_FIELD_VALIDATE_PRESENCE_DEFAULT = "warn";

    String OPENSEARCH_READ_FIELD_INCLUDE = "opensearch.read.field.include";
    String OPENSEARCH_READ_FIELD_EXCLUDE = "opensearch.read.field.exclude";

    String OPENSEARCH_READ_FIELD_AS_ARRAY_INCLUDE = "opensearch.read.field.as.array.include";
    String OPENSEARCH_READ_FIELD_AS_ARRAY_EXCLUDE = "opensearch.read.field.as.array.exclude";

    String OPENSEARCH_READ_SOURCE_FILTER = "opensearch.read.source.filter";

    /** Metadata */
    String OPENSEARCH_READ_METADATA = "opensearch.read.metadata";
    String OPENSEARCH_READ_METADATA_DEFAULT = "false";
    String OPENSEARCH_READ_METADATA_FIELD = "opensearch.read.metadata.field";
    String OPENSEARCH_READ_METADATA_FIELD_DEFAULT = "_metadata";
    String OPENSEARCH_READ_METADATA_VERSION = "opensearch.read.metadata.version";
    String OPENSEARCH_READ_METADATA_VERSION_DEFAULT = "false";
    String OPENSEARCH_READ_UNMAPPED_FIELDS_IGNORE = "opensearch.read.unmapped.fields.ignore";
    String OPENSEARCH_READ_UNMAPPED_FIELDS_IGNORE_DEFAULT = "true";


    /** Operation types */
    String OPENSEARCH_WRITE_OPERATION = "opensearch.write.operation";
    String OPENSEARCH_OPERATION_INDEX = "index";
    String OPENSEARCH_OPERATION_CREATE = "create";
    String OPENSEARCH_OPERATION_UPDATE = "update";
    String OPENSEARCH_OPERATION_UPSERT = "upsert";
    String OPENSEARCH_OPERATION_DELETE = "delete";
    String OPENSEARCH_WRITE_OPERATION_DEFAULT = OPENSEARCH_OPERATION_INDEX;

    String OPENSEARCH_UPDATE_RETRY_ON_CONFLICT = "opensearch.update.retry.on.conflict";
    String OPENSEARCH_UPDATE_RETRY_ON_CONFLICT_DEFAULT = "0";

    String OPENSEARCH_UPDATE_SCRIPT_FILE = "opensearch.update.script.file";
    String OPENSEARCH_UPDATE_SCRIPT_INLINE = "opensearch.update.script.inline";
    String OPENSEARCH_UPDATE_SCRIPT_UPSERT = "opensearch.update.script.upsert";
    String OPENSEARCH_UPDATE_SCRIPT_UPSERT_DEFAULT = "false";
    String OPENSEARCH_UPDATE_SCRIPT_STORED = "opensearch.update.script.stored";
    String OPENSEARCH_UPDATE_SCRIPT_LEGACY = "opensearch.update.script";
    String OPENSEARCH_UPDATE_SCRIPT_LANG = "opensearch.update.script.lang";
    String OPENSEARCH_UPDATE_SCRIPT_PARAMS = "opensearch.update.script.params";
    String OPENSEARCH_UPDATE_SCRIPT_PARAMS_JSON = "opensearch.update.script.params.json";

    /** Output options **/
    String OPENSEARCH_OUTPUT_JSON = "opensearch.output.json";
    String OPENSEARCH_OUTPUT_JSON_DEFAULT = "no";

    /** Network options */
    String OPENSEARCH_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT = "opensearch.net.transport.pooling.expiration.timeout";
    String OPENSEARCH_NET_TRANSPORT_POOLING_EXPIRATION_TIMEOUT_DEFAULT = "5m";

    // SSL
    String OPENSEARCH_NET_USE_SSL = "opensearch.net.ssl";
    String OPENSEARCH_NET_USE_SSL_DEFAULT = "false";

    String OPENSEARCH_NET_SSL_PROTOCOL = "opensearch.net.ssl.protocol";
    String OPENSEARCH_NET_SSL_PROTOCOL_DEFAULT = "TLS"; // SSL as an alternative

    String OPENSEARCH_NET_SSL_KEYSTORE_LOCATION = "opensearch.net.ssl.keystore.location";
    String OPENSEARCH_NET_SSL_KEYSTORE_TYPE = "opensearch.net.ssl.keystore.type";
    String OPENSEARCH_NET_SSL_KEYSTORE_TYPE_DEFAULT = "JKS"; // PKCS12 could also be used
    String OPENSEARCH_NET_SSL_KEYSTORE_PASS = "opensearch.net.ssl.keystore.pass";

    String OPENSEARCH_NET_SSL_TRUST_STORE_LOCATION = "opensearch.net.ssl.truststore.location";
    String OPENSEARCH_NET_SSL_TRUST_STORE_PASS = "opensearch.net.ssl.truststore.pass";

    String OPENSEARCH_NET_SSL_CERT_ALLOW_SELF_SIGNED = "opensearch.net.ssl.cert.allow.self.signed";
    String OPENSEARCH_NET_SSL_CERT_ALLOW_SELF_SIGNED_DEFAULT = "false";

    String OPENSEARCH_NET_HTTP_HEADER_PREFIX = "opensearch.net.http.header.";
    String OPENSEARCH_NET_HTTP_HEADER_OPAQUE_ID = OPENSEARCH_NET_HTTP_HEADER_PREFIX + "X-Opaque-ID";
    String OPENSEARCH_NET_HTTP_HEADER_USER_AGENT = OPENSEARCH_NET_HTTP_HEADER_PREFIX + "User-Agent";
    String OPENSEARCH_NET_HTTP_AUTH_USER = "opensearch.net.http.auth.user";
    String OPENSEARCH_NET_HTTP_AUTH_PASS = "opensearch.net.http.auth.pass";

    String OPENSEARCH_NET_SPNEGO_AUTH_OPENSEARCH_PRINCIPAL = "opensearch.net.spnego.auth.opensearch.principal";
    String OPENSEARCH_NET_SPNEGO_AUTH_MUTUAL = "opensearch.net.spnego.auth.mutual";
    String OPENSEARCH_NET_SPNEGO_AUTH_MUTUAL_DEFAULT = "false";

    String OPENSEARCH_NET_PROXY_HTTP_HOST = "opensearch.net.proxy.http.host";
    String OPENSEARCH_NET_PROXY_HTTP_PORT = "opensearch.net.proxy.http.port";
    String OPENSEARCH_NET_PROXY_HTTP_USER = "opensearch.net.proxy.http.user";
    String OPENSEARCH_NET_PROXY_HTTP_PASS = "opensearch.net.proxy.http.pass";
    String OPENSEARCH_NET_PROXY_HTTP_USE_SYSTEM_PROPS = "opensearch.net.proxy.http.use.system.props";
    String OPENSEARCH_NET_PROXY_HTTP_USE_SYSTEM_PROPS_DEFAULT = "yes";

    String OPENSEARCH_NET_PROXY_HTTPS_HOST = "opensearch.net.proxy.https.host";
    String OPENSEARCH_NET_PROXY_HTTPS_PORT = "opensearch.net.proxy.https.port";
    String OPENSEARCH_NET_PROXY_HTTPS_USER = "opensearch.net.proxy.https.user";
    String OPENSEARCH_NET_PROXY_HTTPS_PASS = "opensearch.net.proxy.https.pass";
    String OPENSEARCH_NET_PROXY_HTTPS_USE_SYSTEM_PROPS = "opensearch.net.proxy.https.use.system.props";
    String OPENSEARCH_NET_PROXY_HTTPS_USE_SYSTEM_PROPS_DEFAULT = "yes";

    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_HOST = "opensearch.net.proxy.socks.host";
    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_PORT = "opensearch.net.proxy.socks.port";
    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_USER = "opensearch.net.proxy.socks.user";
    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_PASS = "opensearch.net.proxy.socks.pass";
    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_USE_SYSTEM_PROPS = "opensearch.net.proxy.socks.use.system.props";
    @Deprecated
    String OPENSEARCH_NET_PROXY_SOCKS_USE_SYSTEM_PROPS_DEFAULT = "yes";

    /** Security options **/
    String OPENSEARCH_SECURITY_AUTHENTICATION = "opensearch.security.authentication";
    String OPENSEARCH_SECURITY_USER_PROVIDER_CLASS = "opensearch.security.user.provider.class";

    /** AWS SigV4 options **/
    String OPENSEARCH_AWS_SIGV4_ENABLED = "opensearch.aws.sigv4.enabled";
    String OPENSEARCH_AWS_SIGV4_ENABLED_DEFAULT = "false";

    String OPENSEARCH_AWS_SIGV4_REGION = "opensearch.aws.sigv4.region";
    String OPENSEARCH_AWS_SIGV4_REGION_DEFAULT = "us-east-1";

    String OPENSEARCH_AWS_SIGV4_SERVICE_NAME = "opensearch.aws.sigv4.service.name";
    String OPENSEARCH_AWS_SIGV4_SERVICE_NAME_DEFAULT = "es";
}