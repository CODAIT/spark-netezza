/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
 *
 * @author Suresh Thalamati
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
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))
    // TODO: Have to set it to the system default.
    val numPartitions = parameters.getOrElse("numPartitions", "4")

    val properties = new Properties() // Additional properties that we will pass to getConnection
    //parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    parameters.foreach{case (k, v) => properties.setProperty(k, v)}

    val parts = NetezzaInputFormat.getDataSlicePartition(
      NetezzaJdbcUtils.getConnector(url, properties)(), numPartitions.toInt)
    NetezzaRelation(url, table, parts, properties, numPartitions.toInt)(sqlContext)
  }
}
