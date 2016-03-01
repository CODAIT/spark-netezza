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

import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import org.apache.spark.sql.types._

/**
  * Converts Netezza format data into Spark SQL row. This is mutable row type
  * to avoid creating too many object of this type for passing each row.
  */
private[netezza] class NetezzaRow(schema: StructType) extends Row {

  override def length: Int = schema.length

  override def get(i: Int): Any = getValue(i)

  override def copy(): Row = {
    val row = new NetezzaRow(this.schema)
    row.netezzaValues = this.netezzaValues.clone()
    row
  }

  val conversionFunctions: Array[String => Any] = schema.fields.map { field =>
    field.dataType match {
      case ByteType => (value: String) => value.toByte
      case BooleanType => (value: String) => parseBoolean(value)
      case DateType => (value: String) => parseDate(value)
      case DoubleType => (value: String) => value.toDouble
      case FloatType => (value: String) => value.toFloat
      case dt: DecimalType => (value: String) => BigDecimal(value)
      case IntegerType => (value: String) => value.toInt
      case LongType => (value: String) => value.toLong
      case ShortType => (value: String) => value.toShort
      case StringType => (value: String) => parseString(value)
      case TimestampType => (value: String) => parseTimestamp(value)
      case _ => throw new IllegalArgumentException(s"Unsupported type: $field.datatype")
    }
  }

  private var netezzaValues: Array[String] = Array.fill(schema.length){null}

  def setValue(i: Int, value: String): Unit = {
    netezzaValues(i) = schema.fields(i).dataType match {
      case StringType => value // empty string is not null for strings.
      case _ => if (value != null && value.isEmpty) null else value
    }
  }

  def getValue(i: Int): Any = {
    val data = netezzaValues(i)
    if (data == null) null else conversionFunctions(i)(data)
  }

  /**
    * Parse the input string specified in the Netezza format into Timestamp.
    * TODO: SimpleDateFormat is not thread safe. Creating new object for each value for time being
    * until we understand if this code called by called by multiple threads are not by Spark RDD.
    */
  def parseTimestamp(value: String): java.sql.Timestamp = {
    var pattern = "yyyy-MM-dd HH:mm" // length 16
    if (value.length() == 19) {
      pattern = "yyyy-MM-dd HH:mm:ss" // length 19
    }
    if (value.length() > 19) {
      pattern = "yyyy-MM-dd HH:mm:ss." // length > 19
      val milisecPositions = value.length() - 20
      for (i <- 0 until milisecPositions) {
        pattern += "S"
      }
    }

    val df: SimpleDateFormat = new SimpleDateFormat(pattern)
    val date = df.parse(value)
    new java.sql.Timestamp(date.getTime())
  }

  /**
    * Parse the date input String in the Netezza format into date.
    *
    * TODO: SimpleDateFormat is not thread safe. Creating new object for each value
    * for time being until we understand if this code called by called by multiple
    * threads are not by Spark RDD.
    */
  def parseDate(value: String): java.sql.Date = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(value)
    new java.sql.Date(date.getTime())
  }

  /**
    * Parse boolean string.
    */
  def parseBoolean(value: String): Boolean = {
    if (value.equals("T")) true else false
  }

  /**
   * Parse string. Null values are written as special pattern using
   * NullValue 'null' opton in the external table definition.
   */
  def parseString(value: String): String = {
    if (value.equals("null")) null else value
  }
}
