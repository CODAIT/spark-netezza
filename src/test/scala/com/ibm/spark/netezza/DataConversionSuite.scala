/**
 * (C) Copyright IBM Corp. 2010, 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.spark.sql.types.{BooleanType, MetadataBuilder, StructField, StructType}
import org.scalatest.FunSuite

/**
 * Test converting from Netezza string data to Spark SQL Row. Netezza external table
 * mechanism writes the data in particular format specified in the external
 * table definition.
 */
class DataConversionSuite extends FunSuite {

  case class Column(name: String, jdbcType: Int,
                    precision: Int = 0, scale: Int = 0, signed: Boolean = false)

  /**
   * Utility routine to build spark sqk schema from jdbc meta data for tesing.
   * @param cols  input column metadata
   * @return spark sqk sql schema corresponding to the input jdbc metadata.
   */
  def buildSchema(cols: Array[Column]): StructType = {
    val fields = new Array[StructField](cols.length)
    var i = 0
    for (col <- cols) {
      val columnType = NetezzaSchema.getSparkSqlType(
        col.jdbcType, col.precision, col.scale, col.signed)
      val metadata = new MetadataBuilder().putString("name", col.name)
      fields(i) = StructField(col.name, columnType, true, metadata.build())
      i = i + 1
    }
    new StructType(fields)
  }

  test("Test varchar data type.") {
    val dbCols = Array(Column("col1", java.sql.Types.VARCHAR))
    val schema = buildSchema(dbCols)
    val nzrow: NetezzaRow = new NetezzaRow(schema)
    val row: Row = nzrow

    nzrow.netezzaValues = Array("mars")
    assert(row.get(0) == "mars")

    // test null
    nzrow.netezzaValues = Array(null)
    assert(row.get(0) == null)
  }

  test("Test date and timestamp datatype.") {
    val dbCols = Array(
      Column("col1", java.sql.Types.DATE), Column("col2", java.sql.Types.TIMESTAMP),
      Column("col3", java.sql.Types.TIMESTAMP), Column("col4", java.sql.Types.TIMESTAMP),
      Column("col5", java.sql.Types.TIMESTAMP), Column("col6", java.sql.Types.TIMESTAMP),
      Column("col7", java.sql.Types.TIMESTAMP))

    val schema = buildSchema(dbCols)
    val nzrow: NetezzaRow = new NetezzaRow(schema)
    nzrow.netezzaValues = Array("1947-08-15", "2000-12-24 01:02", "1901-12-24 01:02:03",
      "1850-01-24 01:02:03.1", "2020-11-24 01:02:03.12", "2015-11-24 01:02:03.123", null)

    // cast it regular row, and call only spark sql row method for verification.
    val row: Row = nzrow
    assert(row.length == 7)
    assert(row.get(0) == java.sql.Date.valueOf("1947-08-15"))
    assert(row.get(1) == java.sql.Timestamp.valueOf("2000-12-24 01:02:00"))
    assert(row.get(2) == java.sql.Timestamp.valueOf("1901-12-24 01:02:03"))
    assert(row.get(3) == java.sql.Timestamp.valueOf("1850-01-24 01:02:03.001"))
    assert(row.get(4) == java.sql.Timestamp.valueOf("2020-11-24 01:02:03.012"))
    assert(row.get(5) == java.sql.Timestamp.valueOf("2015-11-24 01:02:03.123"))
    assert(row.get(6) == null.asInstanceOf[java.sql.Timestamp])
  }

  test("Test Boolean datatypes") {
    val dbCols = Array(Column("col1", java.sql.Types.BOOLEAN))
    val schema = buildSchema(dbCols)
    val nzrow: NetezzaRow = new NetezzaRow(schema)
    // cast it regular row, and call only spark sql row method for verification.
    val row: Row = nzrow
    assert(row.length == 1)
    for (value <- List("T", "F", null)) {
      nzrow.netezzaValues = Array(value)
      val expValue = value match {
        case "T" => true
        case "F" => false
        case null => null.asInstanceOf[BooleanType]
      }
      assert(row.get(0) == expValue)
    }
  }

  test("Test integer datatypes") {
    val dbCols = Array(Column("col1", java.sql.Types.TINYINT),
      Column("col2", java.sql.Types.SMALLINT),
      Column("col3", java.sql.Types.INTEGER),
      Column("col4", java.sql.Types.BIGINT),
      Column("col4", java.sql.Types.BIGINT, 0, 0, true))

    val schema = buildSchema(dbCols)
    val nzrow: NetezzaRow = new NetezzaRow(schema)
    // cast it regular row, and call only spark sql row method for verification.
    val row: Row = nzrow

    nzrow.netezzaValues = Array(
      "10", "32767", "2147483647", "9223372036854775807", "-9223372036854775808")
    assert(row.length == 5)
    assert(row.get(0) == 10)
    assert(row.get(1) == 32767)
    assert(row.get(2) == 2147483647)
    assert(row.get(3) == 9223372036854775807L)
    assert(row.get(4) == -9223372036854775808L)
  }

  test("Test decimal data types") {
    val dbCols = Array(Column("col1", java.sql.Types.FLOAT),
      Column("col2", java.sql.Types.DOUBLE),
      Column("col3", java.sql.Types.NUMERIC),
      Column("col4", java.sql.Types.NUMERIC, 5, 3),
      Column("col5", java.sql.Types.DECIMAL),
      Column("col6", java.sql.Types.DECIMAL, 4, 2))

    val schema = buildSchema(dbCols)
    val expSchema = Array("StructField(col1,FloatType,true)",
      "StructField(col2,DoubleType,true)",
      "StructField(col3,DecimalType(38,18),true)",
      "StructField(col4,DecimalType(5,3),true)",
      "StructField(col5,DecimalType(38,18),true)",
      "StructField(col6,DecimalType(4,2),true)"

    )
    for ((colSchema, expColSchema) <- (schema zip expSchema)) {
      assert(colSchema.toString() == expColSchema)
    }


    val nzrow: NetezzaRow = new NetezzaRow(schema)
    // cast it regular row, and call only spark sql row method for verification.
    val row: Row = nzrow

    nzrow.netezzaValues = Array("1.2", "3.3", "3434.443", "99.1234", "5.3256789", "3456.22")
    assert(row.length == 6)
    assert(row.get(0) == 1.2f)
    assert(row.get(1) == 3.3d)
    assert(row.get(2) == BigDecimal("3434.443"))
    assert(row.get(3) == BigDecimal("99.1234"))
    assert(row.get(4) == BigDecimal("5.3256789"))
    assert(row.get(5) == BigDecimal("3456.22"))
  }

}
