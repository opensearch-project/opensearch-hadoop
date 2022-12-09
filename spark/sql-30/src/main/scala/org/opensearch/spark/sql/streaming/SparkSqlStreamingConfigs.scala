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

package org.opensearch.spark.sql.streaming

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.internal.SQLConf
import org.opensearch.hadoop.OpenSearchHadoopIllegalArgumentException
import org.opensearch.hadoop.cfg.Settings
import org.opensearch.hadoop.util.unit.TimeValue

/**
 * Configurations specifically used for Spark Structured Streaming
 */
object SparkSqlStreamingConfigs {

  val OPENSEARCH_SINK_LOG_ENABLE: String = "opensearch.spark.sql.streaming.sink.log.enabled"
  val OPENSEARCH_SINK_LOG_ENABLE_DEFAULT: Boolean = true

  val OPENSEARCH_SINK_LOG_PATH: String = "opensearch.spark.sql.streaming.sink.log.path"

  val OPENSEARCH_INTERNAL_APP_NAME: String = "opensearch.internal.spark.sql.streaming.appName"
  val OPENSEARCH_INTERNAL_APP_ID: String = "opensearch.internal.spark.sql.streaming.appID"
  val OPENSEARCH_INTERNAL_QUERY_NAME: String = "opensearch.internal.spark.sql.streaming.queryName"
  val OPENSEARCH_INTERNAL_USER_CHECKPOINT_LOCATION: String = "opensearch.internal.spark.sql.streaming.userCheckpointLocation"
  val OPENSEARCH_INTERNAL_SESSION_CHECKPOINT_LOCATION: String = "opensearch.internal.spark.sql.streaming.sessionCheckpointLocation"

  val OPENSEARCH_SINK_LOG_CLEANUP_DELAY: String = "opensearch.spark.sql.streaming.sink.log.cleanupDelay"
  val OPENSEARCH_SINK_LOG_CLEANUP_DELAY_DEFAULT: Long = TimeUnit.MINUTES.toMillis(10)

  val OPENSEARCH_SINK_LOG_DELETION: String = "opensearch.spark.sql.streaming.sink.log.deletion"
  val OPENSEARCH_SINK_LOG_DELETION_DEFAULT: Boolean = true

  val OPENSEARCH_SINK_LOG_COMPACT_INTERVAL: String = "opensearch.spark.sql.streaming.sink.log.compactInterval"
  val OPENSEARCH_SINK_LOG_COMPACT_INTERVAL_DEFAULT: Int = 10

  /**
   * Determines if we should use the commit log for writes, or if we should go without one.
   * @param settings connector settings
   * @return true if we should use the commit log, false if we should not
   */
  def getSinkLogEnabled(settings: Settings): Boolean = {
    Option(settings.getProperty(OPENSEARCH_SINK_LOG_ENABLE)).map(_.toBoolean)
      .getOrElse(OPENSEARCH_SINK_LOG_ENABLE_DEFAULT)
  }

  /**
   * Determines the location of the streaming commit log.
   * @param settings connector settings
   * @return the location to use as the commit log
   */
  def constructCommitLogPath(settings: Settings): String = {
    val logPath = getLogPath(settings)
    val userCheckpointLocation = getUserSpecifiedCheckpointLocation(settings)
    val sessionCheckpointLocation = getSessionCheckpointLocation(settings)
    val queryName = getQueryName(settings)

    (logPath, userCheckpointLocation, sessionCheckpointLocation, queryName) match {
      // Case 1) /{log.path}/{batchFiles}
      // If a user gives an explicit log location with OPENSEARCH_SINK_LOG_PATH, then it's fine to store the commit
      // files in the root of that. That directory should be just for us.
      case (Some(explicitPath), _, _, _) => explicitPath
      // Case 2) /{checkpointLocation}/sinks/opensearch
      // Don't store commit files for the log in the root of the checkpoint location; There are other directories
      // inside that root like '/sources', '/metadata', '/state', '/offsets', etc.
      // Instead, nest it under '/sinks/opensearch'.
      case (None, Some(userCheckpoint), _, _) => s"$userCheckpoint/sinks/opensearch"
      // Case 3) /{sessionCheckpointLocation}/{UUID}
      // Spark lets you define a common location to store checkpoint locations for an entire session. Checkpoint
      // locations are then keyed by their query names. In the event of no query name being present, the query manager
      // creates a random UUID to store the checkpoint in. Not great, since you can't recover easily, but we'll follow
      // suit anyway.
      case (None, None, Some(sessionCheckpoint), None) => s"$sessionCheckpoint/${UUID.randomUUID().toString}/sinks/opensearch"
      // Case 4) /{sessionCheckpointLocation}/{queryName}
      // Same as above, but the query name is specified.
      case (None, None, Some(sessionCheckpoint), Some(query)) => s"$sessionCheckpoint/$query/sinks/opensearch"
      // Case 5) throw because we don't know where to store things...
      case (None, None, None, _) => throw new OpenSearchHadoopIllegalArgumentException(
        "Could not determine path for the OpenSearch commit log. Specify the commit log location by setting the " +
        "[checkpointLocation] option on your DataStreamWriter. If you do not want to persist the OpenSearch " +
        "commit log in the regular checkpoint location for your streaming query then you can specify a location to " +
        s"store the log with [$OPENSEARCH_SINK_LOG_PATH], or disable the commit log by setting [$OPENSEARCH_SINK_LOG_ENABLE] to false.")
    }
  }

  /**
   * The log path, if set, is the complete log path to be used for the commit log.
   * @param settings connector settings
   * @return either Some log path or None
   */
  def getLogPath(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_SINK_LOG_PATH))
  }

  /**
   * The name of the current Spark application, if set
   * @param settings connector settings
   * @return Some name or None
   */
  def getAppName(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_INTERNAL_APP_NAME))
  }

  /**
   * The ID of the current Spark application, if set
   * @param settings connector settings
   * @return Some id or None
   */
  def getAppId(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_INTERNAL_APP_ID))
  }

  /**
   * The name of the current Spark Streaming Query, if set
   * @param settings connector settings
   * @return Some query name or None
   */
  def getQueryName(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_INTERNAL_QUERY_NAME))
  }

  /**
   * The name of the user specified checkpoint location for the current Spark Streaming Query, if set
   * @param settings connector settings
   * @return Some checkpoint location or None
   */
  def getUserSpecifiedCheckpointLocation(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_INTERNAL_USER_CHECKPOINT_LOCATION))
  }

  /**
   * The name of the default session checkpoint location, if set
   * @param settings connector settings
   * @return Some checkpoint location or None
   */
  def getSessionCheckpointLocation(settings: Settings): Option[String] = {
    Option(settings.getProperty(OPENSEARCH_INTERNAL_SESSION_CHECKPOINT_LOCATION))
  }

  /**
   * The number of milliseconds to wait before cleaning up compacted log files
   * @param settings connector settings
   * @return time in millis if set, or the default delay
   */
  def getFileCleanupDelayMs(settings: Settings): Long =
    Option(settings.getProperty(OPENSEARCH_SINK_LOG_CLEANUP_DELAY)).map(TimeValue.parseTimeValue(_).getMillis)
      .orElse(SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.defaultValue)
      .getOrElse(OPENSEARCH_SINK_LOG_CLEANUP_DELAY_DEFAULT)

  /**
   *
   * @param settings connector settings
   * @return
   */
  def getIsDeletingExpiredLog(settings: Settings): Boolean =
    Option(settings.getProperty(OPENSEARCH_SINK_LOG_DELETION)).map(_.toBoolean)
      .orElse(SQLConf.FILE_SINK_LOG_DELETION.defaultValue)
      .getOrElse(OPENSEARCH_SINK_LOG_DELETION_DEFAULT)

  /**
   *
   * @param settings connector settings
   * @return
   */
  def getDefaultCompactInterval(settings: Settings): Int =
    Option(settings.getProperty(OPENSEARCH_SINK_LOG_COMPACT_INTERVAL)).map(_.toInt)
      .orElse(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.defaultValue)
      .getOrElse(OPENSEARCH_SINK_LOG_COMPACT_INTERVAL_DEFAULT)

}