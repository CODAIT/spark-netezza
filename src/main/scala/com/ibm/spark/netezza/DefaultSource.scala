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

import java.util.Properties
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.sources.{DataSourceRegister, BaseRelation, RelationProvider}

/**
  * Implements base relation for Netezza data source. This relation is used when user
  * specifies "com.ibm.spark.netezza' or 'netezza'  as data format for the data frame
  * reader or in the DDL operation with USING clause.
  *
  * Netezza data source implementations is based on in-built Spark SQL JDBC datasource.
  */
class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "netezza"

  /**
    * Returns a Netezza data source relation with the given parameters.
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'Netezza database url' not specified"))
    val (table, isQuery) = parameters.get("dbtable").map(table => (table, false)).orElse {
      parameters.get("query")
        .map(q => (s"($q) as src", true))
        .orElse(sys.error("Option 'dbtable/query' should be specified."))
    }.get

    // TODO: Have to set it to the system default.
    // For query default is 1, when fetching from a table defauilt is 4. Data slice ca
    // can be used for partitioning when table is specified.
    val numPartitions = parameters.getOrElse("numPartitions", if (isQuery) "1" else "4").toInt

    val partitionCol = parameters.get("partitioncol")
    val lowerBound = parameters.get("lowerbound")
    val upperBound = parameters.get("upperbound")

    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach { case (k, v) => properties.setProperty(k, v) }

    val conn = NetezzaJdbcUtils.getConnector(url, properties)()
    val parts = try {
      if (partitionCol.isDefined || isQuery) {
        if (isQuery && numPartitions > 1 && !partitionCol.isDefined) {
          throw new IllegalArgumentException("Partition column should be specified or" +
            " number of partitions should be set to 1 with the query option.")
        }
        val partnInfo = PartitioningInfo(partitionCol, lowerBound, upperBound, numPartitions)
        NetezzaInputFormat.getColumnPartitions(conn, table, partnInfo)
      } else {
        // Partitions based on the data slices.
        NetezzaInputFormat.getDataSlicePartition(conn, numPartitions)
      }
    } finally { conn.close() }

    NetezzaRelation(url, table, parts, properties, numPartitions)(sqlContext)
  }
}
