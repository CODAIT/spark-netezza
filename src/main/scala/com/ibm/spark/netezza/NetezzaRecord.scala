package com.ibm.spark.netezza

import java.text.SimpleDateFormat

/**
 * Netezza record holds one row from the netezza table. Provides methods to convert
 * from Strings into the data frame data type.
 *
 * @author Suresh Thalamati
 */
class NetezzaRecord(row: Array[String]) {

  def getBytes(pos: Int) = {
    if (row(pos) != null) row(pos).getBytes else null
  }

  def getTimestamp(pos: Int) = {
    if (row(pos) == null) {
      null.asInstanceOf[java.sql.Timestamp]
    } else {
      val source = row(pos)
      var pattern = "yyyy-MM-dd HH:mm" // length 16
      if (source.length() == 19) {
        pattern = "yyyy-MM-dd HH:mm:ss" // length 19
      }
      if (source.length() > 19) {
        pattern = "yyyy-MM-dd HH:mm:ss." // length > 19
        val milisecPositions = source.length() - 20
        for (i <- 0 until milisecPositions) {
          pattern += "S"
        }
      }
      val df: SimpleDateFormat = new SimpleDateFormat(pattern)
      val date = df.parse(source)
      new java.sql.Timestamp(date.getTime())
    }
  }

  def getLong(pos: Int): Long = {
    if (row(pos) != null) row(pos).toLong else null.asInstanceOf[Long]
  }

  def getInt(pos: Int): Int = {
    if (row(pos) != null) row(pos).toInt else null.asInstanceOf[Int]
  }

  def getFloat(pos: Int): Float = {
    if (row(pos) != null) row(pos).toFloat else null.asInstanceOf[Float]
  }

  def getBigDecimal(pos: Int): BigDecimal = {
    if (row(pos) != null) BigDecimal(row(pos)) else null.asInstanceOf[BigDecimal]
  }

  def getDate(pos: Int): java.sql.Date = {
    if (row(pos) == null) {
      null.asInstanceOf[java.sql.Date]
    } else {

      val pattern = "yyyy-MM-dd"
      val df = new SimpleDateFormat(pattern)
      val date = df.parse(row(pos))
      new java.sql.Date(date.getTime())

    }
  }


  def getBoolean(pos: Int): Boolean = {
    if (row(pos) == null) {
      null.asInstanceOf[Boolean]
    } else {
      if (row(pos).equals("T")) true else false
    }
  }

  def getString(index: Int): String = {
    if (row(index) != null) row(index) else null.asInstanceOf[String]
  }

  def getDouble(index: Int): Double = {
    if (row(index) != null) row(index).toDouble else null.asInstanceOf[Double]
  }

  def getInteger(index: Int): Int = {
    if (row(index) != null) row(index).toInt else null.asInstanceOf[Int]
  }

  def wasNull(pos: Int): Boolean = {
    row(pos) == null
  }
}