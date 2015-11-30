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

import org.apache.spark.sql.Row

/**
 * Test parsing of the data in the netezza format into spark sql row.
 */
class RecordParserSuite extends NetezzaBaseSuite {

  test("testing netezza record parser") {

    val dbCols = Array(
      Column("PRODUCTNUMBER", java.sql.Types.INTEGER),
      Column("INTRODUCTIONDATE", java.sql.Types.TIMESTAMP),
      Column("PRODUCTNAME", java.sql.Types.VARCHAR, 50, 0, false),
      Column("PRODUCTTYPECODE", java.sql.Types.INTEGER),
      Column("PRODUCTIONCOST", java.sql.Types.DOUBLE, 15, 0, false),
      Column("MARGIN", java.sql.Types.DOUBLE, 15, 0, false),
      Column("PICTURE", java.sql.Types.VARCHAR, 100, 0, true),
      Column("PICTUREURL", java.sql.Types.VARCHAR, 100, 0, true),
      Column("DESCRIPTION", java.sql.Types.VARCHAR, 255, 0, true))

    val schema = buildSchema(dbCols)

    val delimiter: Char = '\001';
    // field delimiter
    val escape: Char = '\\'
    val recordParser = new NetezzaRecordParser(delimiter, escape, schema)
    val del: Char = '\001';
    val input = s"""111${delimiter}1998-12-15 00:00${delimiter}
        |Blue Steel Max Putter${delimiter}20${del}81.8${del}0.55${delimiter}
        |P111GE5PT20${delimiter}/cer/samples/images/P111GE5PT20.jpg${delimiter}
        |Putter head is composed from a sin
        |gle piece of the softest carbon steel for optimum feel."""
      .stripMargin.replaceAll("\n", "")

    val row: Row = recordParser.parse(input)
    assert(row.length == 9)
    assert(row.get(0) == 111)
    assert(row.get(1) == java.sql.Timestamp.valueOf("1998-12-15 00:00:00"))
    assert(row.get(2) == "Blue Steel Max Putter")
    assert(row.get(3) == 20)
    assert(row.get(4) == 81.8d)
    assert(row.get(5) == 0.55d)
    assert(row.get(6) == "P111GE5PT20")
    assert(row.get(7) == "/cer/samples/images/P111GE5PT20.jpg")
    assert(row.get(8) ==
      "Putter head is composed from a single piece of the softest carbon steel for optimum feel.")
  }

}
