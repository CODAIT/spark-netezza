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

import java.sql.Connection
import org.apache.spark.Partition
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

/**
  * Instructions on how to partition the table among workers.
  */
private[netezza] case class PartitioningInfo(column: Option[String],
                                          lowerBound: Option[String],
                                          upperBound: Option[String],
                                          numPartitions: Int)

/**
  * Get information about data slices.
  */
private[netezza] object NetezzaInputFormat {

  private val log = LoggerFactory.getLogger(getClass)

  def getParitionPredicate(start: Int, end: Int): String = {
    s"DATASLICEID BETWEEN $start AND $end";
  }

  /**
    * Get number of data slices configured in the database.
    *
    * @param conn connection to the database.
    * @return number of of data slices
    */
  def getNumberDataSlices(conn: Connection): Integer = {

    // query to get maximum number of data slices in the database.
    val query = "select max(ds_id) from  _v_dslice"
    val stmt = conn.prepareStatement(query)
    try {
      val rs = stmt.executeQuery()
      try {

        val numberDataSlices = if (rs.next) rs.getInt(1) else 0
        if (numberDataSlices == 0) {
          // there should always be some data slices with netezza
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
    if (numPartitions <= 1 || numberDataSlices <= 1) {
      Array[Partition](NetezzaPartition(null, 0))
    } else {
      val ans = new ArrayBuffer[Partition]()
      var partitionIndex = 0
      // if there are more partions than the data slice, assign one data slice per partition
      if (numberDataSlices <= numPartitions) {
        // one data slice per mapper
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

  /**
    * Get column partitions based on the user specified column.
    */
  def getColumnPartitions(conn: Connection, table: String,
                          partitionInfo: PartitioningInfo): Array[Partition] = {
    if (partitionInfo.numPartitions <= 1 || !partitionInfo.column.isDefined) {
      Array[Partition](NetezzaPartition(null, 0))
    } else {
      val (min, max) = NetezzaJdbcUtils.getPartitionColumnBoundaryValues(conn, table, partitionInfo)
      getIntegerColumnPartition(partitionInfo.column, min, max, partitionInfo.numPartitions)
    }
  }

  /**
    * Given a partitioning schematic (a column of integral type, a number of
    * partitions, and upper and lower bounds on the column's value), generate
    * WHERE clauses for each partition so that each row in the table appears
    * exactly once.  The parameters minValue and maxValue are advisory in that
    * incorrect values may cause the partitioning to be poor, but no data
    * will fail to be represented.
    *
    * Null value predicate is added to the first partition where clause to include
    * the rows with null value for the partitions column.
    */
  def getIntegerColumnPartition(partitioncolumn: Option[String],
                                lowerBound: Long,
                                upperBound: Long,
                                userSpecifiedNumPartitions: Int): Array[Partition] = {


    require(upperBound > lowerBound,
      "lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val numPartitions =
      if ((upperBound - lowerBound) >= userSpecifiedNumPartitions) {
        userSpecifiedNumPartitions
      } else {
        log.warn("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${userSpecifiedNumPartitions}; Lower bound: $lowerBound; " +
          s"Upper bound: $upperBound.")

        upperBound - lowerBound
      }

    if (numPartitions <= 1 || lowerBound == upperBound) {
      return Array[Partition](NetezzaPartition(null, 0))
    }

    val column = partitioncolumn.get

    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions
    var i: Int = 0
    var currentValue: Long = lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val uBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += NetezzaPartition(whereClause, i)
      i = i + 1
    }
    ans.toArray
  }
}
