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

package com.ibm.spark.netezza.integration

import java.sql.Timestamp

import org.apache.spark.sql.{Row, DataFrame}
import org.netezza.error.NzSQLException
import org.scalatest.Ignore

class IntegrationTestSuite extends IntegrationSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()

    def dropTable(tableName: String): Unit = {
      try {
        conn.createStatement().executeUpdate(s"drop table $tableName")
      } catch {
        case e: NzSQLException => print(e.toString)
      }
    }

    def createTable(tableName: String): Unit = {
        conn.createStatement().executeUpdate(
          s"""
             |create table $tableName (
             |boolCol boolean,
             |intCol int,
             |byteIntCol int1,
             |smallIntCol int2,
             |bigInt int8,
             |floatCol float,
             |doubleCol double,
             |charCol char,
             |varcharCol2 varchar(6400),
             |tsCol timestamp
             |)
      """.stripMargin
        )

      // scalastyle:off
      conn.createStatement().executeUpdate(
        s"""
           |insert into $tableName values
           |(false, 2147483647, -128, 32767, 2147483648, 3.4, 5.6, 'a', 'rAnD0m 5Tring',
           |'1969-12-31 16:00:00.0');
           |insert into $tableName values
           |(null, null, null, null, null, null, null, null, null, null)
           """.stripMargin
      )
      // scalastyle:on
    }

    dropTable(testTable)
    createTable(testTable)
  }

  override def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

  private def defaultOpts() = {
    Map("url" -> testURL,
      "user" -> user,
      "password" -> password,
      "dbtable" -> testTable,
      "numPartitions" -> Integer.toString(numPartitions))
  }

  test("Test load netezza to a DataFrame") {
    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(defaultOpts).load()
    verifyAnswer(testDf, TestUtils.expectedData)
  }

  test("Test mixed case table identifiers") {
    val tabName = "\"mixCaseTab\""
    withTable(tabName) {
      executeJdbcStmt(s"create table $tabName(id int , name varchar(10))")
      executeJdbcStmt(s"insert into $tabName values(1 , 'John Doe')")
      val opts = defaultOpts + ("dbTable" -> tabName)
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      val expected = Seq(Row(1, "John Doe"))
      verifyAnswer(testDf, expected)
    }
  }

  test("Test mixed case column names") {
    val opts = defaultOpts + ("dbTable" -> "\"mxCaseColsTab\"")
    val tabName = "mixCaseColTab"
    withTable(tabName) {
      executeJdbcStmt(s"""create table $tabName(id int , "Name" varchar(10))""")
      executeJdbcStmt(s"insert into $tabName values(1 , 'John Doe')")
      val opts = defaultOpts + ("dbTable" -> tabName)
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      assert(testDf.schema.fieldNames(1) == "Name")
      val expected = Seq(Row(1, "John Doe"))
      verifyAnswer(testDf, expected)
    }
  }

  test("Test multiple rows with simple string data.") {
    val opts = defaultOpts + ("dbTable" -> "\"mxCaseColsTab\"")
    val tabName = "mixCaseColTab"
    withTable(tabName) {
      executeJdbcStmt(s"""create table $tabName(id int , "Name" varchar(20))""")
      val opts = defaultOpts + ("dbTable" -> tabName)
      executeJdbcStmt(
        s"""insert into $tabName values(1 , 'John Doe')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(2 , 'Mike Doe')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(3 , 'Robert Doe')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(4, 'Kathy Doe')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(5, 'Samantha\tDoe')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(6, 'Dennis Doe')""".stripMargin)

      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      assert(testDf.schema.fieldNames(1) == "Name")
      val expected = Seq(
        Row(1, "John Doe"),
        Row(2, "Mike Doe"),
        Row(3, "Robert Doe"),
        Row(4, "Kathy Doe"),
        Row(5, "Samantha\tDoe"),
        Row(6, "Dennis Doe"))
      verifyAnswer(testDf, expected)
    }
  }

  test("Test control character in the string data.") {
    val opts = defaultOpts + ("dbTable" -> "\"mxCaseColsTab\"")
    val tabName = "mixCaseColTab"
    withTable(tabName) {
      executeJdbcStmt(s"""create table $tabName(id int , "Name" varchar(20))""")
      val opts = defaultOpts + ("dbTable" -> tabName)
      executeJdbcStmt(
        s"""insert into $tabName values(1 , 'John\nDoe1')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(2 , 'Mike\rDoe2')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(3 , 'Robert\n\rDoe3')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(4, 'Kathy\r\nDoe4')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(5, 'Samantha\u001Doe5')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(6, 'Dennis\n \t \r\n Doe6')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(7, 'Thomas\\Do\\\\e7')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(8, 'Dilip"Biswal8')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(9, 'Xiao''Li9')""".stripMargin)

      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      assert(testDf.schema.fieldNames(1) == "Name")
      val expected = Seq(
        Row(1, "John\nDoe1"),
        Row(2, "Mike\rDoe2"),
        Row(3, "Robert\n\rDoe3"),
        Row(4, "Kathy\r\nDoe4"),
        Row(5, "Samantha\u001Doe5"),
        Row(6, "Dennis\n \t \r\n Doe6"),
        Row(7, "Thomas\\Do\\\\e7"),
        Row(8, "Dilip\"Biswal8"),
        Row(9, "Xiao'Li9"))
      verifyAnswer(testDf, expected)
    }
  }

  test("Test escape char in the string data.") {
    val opts = defaultOpts + ("dbTable" -> "\"mxCaseColsTab\"")
    val tabName = "mixCaseColTab"
    withTable(tabName) {
      executeJdbcStmt(s"""create table $tabName(id int , "Name" varchar(20))""")
      val opts = defaultOpts + ("dbTable" -> tabName)
      executeJdbcStmt(
        s"""insert into $tabName values(1 , 'John\\Doe1')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(2 , '\\Mike Doe2')""".stripMargin)
      executeJdbcStmt(
        s"""insert into $tabName values(3 , 'Xin Wu\\')""".stripMargin)
      executeJdbcStmt(s"insert into $tabName values(4 , 'abcd\nefg\thij\r\n\\')")
      executeJdbcStmt(s"insert into $tabName values(5 , 'klmn')")
      executeJdbcStmt(s"insert into $tabName values(6 , '\nopqrst')")
      executeJdbcStmt(s"insert into $tabName values(7 , 'uvwxyz\n')")

      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      assert(testDf.schema.fieldNames(1) == "Name")
      val expected = Seq(
        Row(1, "John\\Doe1"),
        Row(2, "\\Mike Doe2"),
        Row(3, "Xin Wu\\"),
        Row(4, "abcd\nefg\thij\r\n\\"),
        Row(5, "klmn"),
        Row(6, "\nopqrst"),
        Row(7, "uvwxyz\n")
      )
      verifyAnswer(testDf, expected)
    }
  }

  test("Test with different partition numbers.") {
    var opts = defaultOpts + ("numPartitions" -> "0")
    var testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, TestUtils.expectedData)

    opts = defaultOpts + ("numPartitions" -> "1")
    testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, TestUtils.expectedData)

    opts = defaultOpts + ("numPartitions" -> "2")
    testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, TestUtils.expectedData)

    opts = defaultOpts + ("numPartitions" -> "3")
    testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, TestUtils.expectedData)

    opts = defaultOpts + ("numPartitions" -> "8")
    testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, TestUtils.expectedData)
  }

  test("test plan outout") {
    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(defaultOpts).load()
    // explain prints output, i could not find a simple way without calling
    // intenal spark classes. Not worth adding spark internal dependencies
    // for a test case. Other option may be to grab the console output.
    testDf.explain(true)
  }

  test("test netezza datasource alias/shortname in format") {
    val testDf = sqlContext.read.format("netezza").options(defaultOpts).load()
    verifyAnswer(testDf, TestUtils.expectedData)
  }

  test("test netezza datasource table with alias/shortname") {
    val opts = defaultOpts()
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE netezza_table
         |USING netezza
         |OPTIONS (
         |url '${opts("url")}',
         |user '${opts("user")}',
         |password '${opts("password")}',
         |dbtable '${opts("dbtable")}')
    """.stripMargin.replaceAll("\n", " ")
    )

    val testDf = sqlContext.sql("SELECT * FROM netezza_table")
    verifyAnswer(testDf, TestUtils.expectedData)
  }

}
