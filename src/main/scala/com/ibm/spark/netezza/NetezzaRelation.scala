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


import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.PrunedFilteredScan

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
 *
 * Netezza relation that fetches only required columns and pushing filters to Netezza data base.
 * Generates schema based on the input table data types in the Netezza database.
 *
 * @author Suresh Thalamati
 */
private[netezza] case class NetezzaRelation(
    url: String,
    table: String,
    parts: Array[Partition],
    properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan {

  override val schema: StructType = NetezzaSchema.getSparkSqlSchema(url, properties, table)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    new NetezzaRDD(
      sqlContext.sparkContext,
      NetezzaJdbcUtils.getConnector(url, properties),
      NetezzaSchema.pruneSchema(schema, requiredColumns),
      table,
      requiredColumns,
      filters,
      parts,
      properties)
  }
}

