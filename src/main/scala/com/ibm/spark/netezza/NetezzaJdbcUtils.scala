/**
  * (C) Copyright IBM Corp. 2015, 2016
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.spark.netezza

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.sources.Filter
import org.slf4j.LoggerFactory

/**
  * Utility jdbc methods to communicate with Netezza. These methods are based on Spark SQL code,
  * and depends on the internal class DriverRegistry.
  */
private[netezza] object NetezzaJdbcUtils {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Given  an url, return a function that loads the
    * specified driver string then returns a connection to the JDBC url.
    * getConnector is run on the driver code, while the function it returns
    * is run on the executor.
    *
    * @param url - The JDBC url to connect to.
    * @return A function that loads the driver and connects to the url.
    */
  def getConnector(url: String, properties: Properties): () => Connection = {
    () => {
      val driver = "org.netezza.Driver"
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          log.error(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }

  def quoteIdentifier(colName: String): String = {
    s"""$colName"""
  }

  def getCountWithFilter(
      url: String,
      properties: Properties,
      table: String,
      filters: Array[Filter]): Long = {
    val whereClause = NetezzaFilters.getFilterClause(filters)
    val countQuery = s"SELECT count(*) FROM $table $whereClause"
    log.info(countQuery)
    val conn = NetezzaJdbcUtils.getConnector(url, properties)
    var count: Long = 0
    try {
      val results = conn().prepareStatement(countQuery).executeQuery()

      if (results.next()) {
        count = results.getLong(1)
      }
      else {
        throw new IllegalStateException("Could not read count from Netezza")
      }
    } finally {
      conn().close()
    }
    count
  }

  object minMax extends Enumeration {
    val Min, Max = Value
  }

  /**
    * Get Partition column boundary values for the specified table/ derived table query
    * from the Netezza database. If user specified the boundary values , this method will
    * returns them instead of querying the Netezza server. Allowing user to specify helps if
    * the derived table query is complex that contains joins.
    */
  def getPartitionColumnBoundaryValues(
    conn: Connection,
    table: String,
    partitioningInfo: PartitioningInfo
    ): (Long, Long) = {

    val (requireMinValue, requireMaxValue) =
      (partitioningInfo.lowerBound, partitioningInfo.upperBound) match {
        case (Some(lower), Some(upper)) => (false, false)
        case (Some(lower), None) => (false, true)
        case (None, Some(upper)) => (true, false)
        case (None, None) => (true, true)
      }

    val partitionCol = partitioningInfo.column.get
    val (minMaxClause, noCols) = (requireMinValue, requireMaxValue) match {
      case (true, true) => (s"min($partitionCol), max($partitionCol)", 2)
      case (true, false) => (s"min($partitionCol)", 1)
      case (false, true) => (s"max($partitionCol)", 1)
      case (false, false) =>
        return (partitioningInfo.lowerBound.get.toLong, partitioningInfo.upperBound.get.toLong)
    }

    val minMaxQuery = s"SELECT $minMaxClause FROM $table"
    log.info(minMaxQuery)
    var count: Long = 0
    try {
      val results = conn.prepareStatement(minMaxQuery).executeQuery()
      if (results.next()) {
        if (noCols == 1) {
          val value = results.getLong(1)
          if (requireMinValue) {
            (results.getLong(1), partitioningInfo.upperBound.get.toLong)
          } else {
            (partitioningInfo.lowerBound.get.toLong, value)
          }
        } else {
          (results.getLong(1), results.getLong(2))
        }
      }
      else {
        throw new RuntimeException("Could not fetch min/max values from Netezza")
      }
    } finally {
      conn.close()
    }
  }
}
