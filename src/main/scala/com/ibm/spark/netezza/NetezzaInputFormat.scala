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

import java.sql.Connection
import org.apache.spark.Partition
import scala.collection.mutable.ArrayBuffer

/**
  * Get information about data slices.
  */
private[netezza] object NetezzaInputFormat {

  def getParitionPredicate(start: Int, end: Int) = {
    s"DATASLICEID BETWEEN $start AND $end";
  }

  /**
    * Get number of data slices configured in the database.
    * @param conn connection to the database.
    * @return number of of data slices
    */
  def getNumberDataSlices(conn: Connection): Integer = {

    //query to get maximum number of data slices in the database.
    val query = "select max(ds_id) from  _v_dslice"
    val stmt = conn.prepareStatement(query)
    try {
      val rs = stmt.executeQuery()
      try {

        val numberDataSlices = if (rs.next) rs.getInt(1) else 0
        if (numberDataSlices == 0) {
          //there should always be some data slices with netezza
          throw new Exception("No data slice ids returned.");
        }
        return numberDataSlices
      } finally {
        rs.close()
      }
    } finally {
      stmt.close
    }
  }

  /**
    * Get partitions mapping to the data slices in the database,
    */
  def getDataSlicePartition(conn: Connection, numPartitions: Int): Array[Partition] = {
    val numberDataSlices = getNumberDataSlices(conn)
    if (numPartitions == 1 || numberDataSlices == 1) {
      Array[Partition](NetezzaPartition(getParitionPredicate(1, numberDataSlices), 0))
    } else {
      val ans = new ArrayBuffer[Partition]()
      var partitionIndex = 0
      //if there are more partions than the data slice, assign one data slice per partition
      if (numberDataSlices <= numPartitions) {
        //one data slice per mapper
        for (sliceId <- 1 to numberDataSlices) {
          ans += NetezzaPartition(getParitionPredicate(sliceId, sliceId), partitionIndex)
          partitionIndex = partitionIndex + 1
        }
      } else {
        val slicesPerPartion: Int = {
          val slices = Math.floor(numberDataSlices / numPartitions).toInt
          if (slices * numPartitions < numberDataSlices) slices + 1 else slices
        }

        var start = 1
        var end = slicesPerPartion

        for (index <- 1 to numPartitions if (start <= numberDataSlices)) {
          ans += NetezzaPartition(getParitionPredicate(start, end), partitionIndex)
          partitionIndex = partitionIndex + 1
          start = end + 1;
          end = if ((start + slicesPerPartion - 1) > numberDataSlices) {
            numberDataSlices
          } else {
            start + slicesPerPartion - 1
          }
        }

      }
      ans.toArray
    }
  }
}

