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

import java.sql.{ResultSetMetaData, Connection, SQLException}
import java.util.Properties
import org.apache.spark.sql.types._

/**
  * Generates schema for the data source by mapping to Netezza jdbc type to Spark sql data types.
  * Spark SQL jdbc datasource mapping is modified to make Netezza specific.
  */
private[netezza] object NetezzaSchema {

  /**
    *
    * Returns spark sql type schema based for the given table.
    *
    * @param url  Connection url to the netezzza database.
    * @param properties  connection properties.
    * @param table The table name of the desired table.
    * @return A StructType giving the table's spark sql schema.
    * @throws SQLException if the table specification is garbage.
    * @throws SQLException if the table contains an unsupported type.
    */
  def getSparkSqlSchema(url: String, properties: Properties, table: String): StructType = {
    val conn: Connection = NetezzaJdbcUtils.getConnector(url, properties)()
    try {
      val rs = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0").executeQuery()
      try {
        val rsmd = rs.getMetaData
        val ncols = rsmd.getColumnCount
        val fields = new Array[StructField](ncols)
        var i = 0
        while (i < ncols) {
          val columnName = rsmd.getColumnLabel(i + 1)
          val dataType = rsmd.getColumnType(i + 1)
          val typeName = rsmd.getColumnTypeName(i + 1)
          val fieldSize = rsmd.getPrecision(i + 1)
          val fieldScale = rsmd.getScale(i + 1)
          val isSigned = rsmd.isSigned(i + 1)
          val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
          val columnType = getSparkSqlType(dataType, fieldSize, fieldScale, isSigned)

          val metadata = new MetadataBuilder().putString("name", columnName)
          fields(i) = StructField(columnName, columnType, nullable, metadata.build())
          i = i + 1
        }
        new StructType(fields)
      } finally {
        rs.close()
      }
    } finally {
      conn.close()
    }
  }


  /**
    * Maps Netezaa JDBC type to a spark sql type.
    *
    * @param jdbcType - Jdbc type for the Netezza column
    * @return The Spark SQL type corresponding to sqlType.
    */
  def getSparkSqlType(
                       jdbcType: Int,
                       precision: Int,
                       scale: Int,
                       signed: Boolean): DataType = {
    val answer = jdbcType match {
      // scalastyle:off
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => FloatType
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + jdbcType)
    answer
  }

  /**
    * Prune all but the specified columns from the Spark SQL schema.
    *
    * @param schema - The Spark sql schema of the mappned netezza table
    * @param columns - The list of desired columns
    *
    * @return A Spark sql schema corresponding to columns in the given order.
    */
  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }
}
