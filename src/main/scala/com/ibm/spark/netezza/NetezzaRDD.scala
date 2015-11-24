/**
 * (C) Copyright IBM Corp. 2010, 2015
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
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * Data corresponding to one partition of a Netezza RDD.
  */
private[netezza] case class NetezzaPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

/**
  * An RDD representing a table in a database accessed via JDBC.  Both the
  * driver code and the workers must be able to access the database; the driver
  * needs to fetch the schema while the workers need to fetch the data.
  */
private[netezza] class NetezzaRDD(
                                   sc: SparkContext,
                                   getConnection: () => Connection,
                                   schema: StructType,
                                   table: String,
                                   columns: Array[String],
                                   filters: Array[Filter],
                                   partitions: Array[Partition],
                                   properties: Properties)
  extends RDD[Row](sc, Nil) {

  /**
    * Retrieve the list of partitions corresponding to this RDD.
    */
  override def getPartitions: Array[Partition] = partitions


  /**
    * Runs the SQL query for the given patitions againest Netezza database , and
    * returns the results as iterator on  spark sql rows.
    */
  override def compute(thePart: Partition, context: TaskContext): Iterator[Row] =
    new Iterator[Row] {
      var closed = false
      var finished = false
      var gotNext = false
      var nextValue: Row = null

      context.addTaskCompletionListener { context => close() }
      val part = thePart.asInstanceOf[NetezzaPartition]
      val conn = getConnection()
      val reader = new NetezzaDataReader(conn, table, columns, filters, part)
      val netezzaRow = new NetezzaRow(schema)

      def getNext(): Row = {
        if (reader.hasNext) {
          val record = reader.next()
          netezzaRow.netezzaValues = record.getNetezzaValues()
          netezzaRow
        } else {
          finished = true
          null.asInstanceOf[Row]
        }
      }

      def close() {
        if (closed) return
        try {
          if (null != reader) {
            reader.close()
          }
        } catch {
          case e: Exception => logWarning("Exception closing Netezza record reader", e)
        }
        try {
          if (null != conn) {
            conn.close()
          }
          logInfo("closed connection")
        } catch {
          case e: Exception => logWarning("Exception closing connection", e)
        }
      }

      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            nextValue = getNext()
            if (finished) {
              close()
            }
            gotNext = true
          }
        }
        !finished
      }

      override def next(): Row = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }
}
