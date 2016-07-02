/**
  * (C) Copyright IBM Corp. 2015, 2016
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

package com.ibm.spark.netezza.integration

import org.apache.spark.sql.{DataFrame, Row}
import org.netezza.error.NzSQLException
import org.apache.spark.SPARK_VERSION

/**
  * Test cases for reading data with filters on table and queries.
  */
class FilterIntegrationTestSuite extends IntegrationSuiteBase {

  val tabName = "staff"
  val expected = Seq(
    Row(1, "John Doe"),
    Row(2, "Jeff Smith"),
    Row(3, "Kathy Saunders"),
    Row(4, null)
  )

  val expectedFiltered = Seq(Row(1, "John Doe"), Row(2, "Jeff Smith"))
  val expected1 = Seq(Row(2, "Jeff Smith"))
  val expected2 = Seq(Row(1, "John Doe"), Row(2, "Jeff Smith"))
  val expected3 = Seq(Row(1, "John Doe"), Row(2, "Jeff Smith"), Row(3, "Kathy Saunders"))
  val expected4 = Seq(Row(2, "Jeff Smith"), Row(3, "Kathy Saunders"), Row(4, null))

  override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      executeJdbcStmt(s"drop table $tabName")
    } catch {
      case e: NzSQLException =>
    }
    executeJdbcStmt(s"create table $tabName(id int , name varchar(20))")
    executeJdbcStmt(s"insert into $tabName values(1 , 'John Doe')")
    executeJdbcStmt(s"insert into $tabName values(2 , 'Jeff Smith')")
    executeJdbcStmt(s"insert into $tabName values(3 , 'Kathy Saunders')")
    executeJdbcStmt(s"insert into $tabName values(4 , null)")
  }

  override def afterAll(): Unit = {
    try {
      executeJdbcStmt(s"DROP TABLE $tabName")
    } finally {
      super.afterAll()
    }
  }

  private def defaultOpts() = {
    Map("url" -> testURL,
      "user" -> user,
      "password" -> password,
      "numPartitions" -> Integer.toString(1))
  }


  test("Test conditional filters on table with single partition") {
    val testDf = getTestdataframe(true, 1)
    testSimpleCondtionFilters(testDf)
    testLogicalFilters(testDf)
    testcomplexFilters(testDf)
  }

  test("Test conditional filters on table with partitions") {
    val testDf = getTestdataframe(true, numPartitions)
    testSimpleCondtionFilters(testDf)
    testLogicalFilters(testDf)
    testcomplexFilters(testDf)
  }

  test("Test conditional filters on query with single partition") {
    val testDf = getTestdataframe(true, 1)
    testSimpleCondtionFilters(testDf)
    testLogicalFilters(testDf)
    testcomplexFilters(testDf)
  }

  test("Test conditional filters on query with partitions") {
    val testDf = getTestdataframe(true, numPartitions)
    testSimpleCondtionFilters(testDf)
    testLogicalFilters(testDf)
    testcomplexFilters(testDf)
  }


  private def testSimpleCondtionFilters(testDf: DataFrame) = {
    // conditional filters
    verifyAnswer(testDf.filter("ID <> 4 AND ID != 3"), expected2)
    verifyAnswer(testDf.filter("ID = 2"), expected1)
    verifyAnswer(testDf.filter("ID < 3 "), expected2)
    verifyAnswer(testDf.filter("ID <=2 "), expected2)
    verifyAnswer(testDf.filter("ID > 0 AND ID < 3 "), expected2)
    verifyAnswer(testDf.filter("ID >= 1 AND ID < 3 "), expected2)
    // Null filters
    if (SPARK_VERSION >= "1.6.0") {
      // there is a bug in older spark version with null safe join
      // resulting in incorrect results.
      verifyAnswer(testDf.filter("NAME <=> null"), Seq(Row(4, null)))
    }
  }

  private def testLogicalFilters(testDf: DataFrame) = {
    verifyAnswer(testDf.filter("NAME is null"), Seq(Row(4, null)))
    verifyAnswer(testDf.filter("NAME is not null"), expected3)
    // String filter operations.
    verifyAnswer(testDf.filter("NAME LIKE 'Jeff%'"), expected1)
    verifyAnswer(testDf.filter("NAME LIKE '%Smith'"), expected1)
    verifyAnswer(testDf.filter("NAME LIKE '%th%'"),
      Seq(Row(2, "Jeff Smith"), Row(3, "Kathy Saunders")))
    verifyAnswer(testDf.filter("NAME LIKE '%%'"), expected3)

    // IN filter
    verifyAnswer(testDf.filter("ID IN (2, 3, 4)"), expected4)
    verifyAnswer(testDf.filter(s"ID IN (2,3) OR NAME IS NULL"), expected4)

    // NOT
    if (SPARK_VERSION >= "1.6.0") {
      // there is a bug in older spark version resultig in pasrsing error.
      verifyAnswer(testDf.filter("NOT ID IN (3, 4)"), expected2)
    }
    verifyAnswer(testDf.filter("NOT (ID = 3 OR ID = 4)"), expected2)
  }

  private def testcomplexFilters(testDf: DataFrame) = {
    // simple OR filter
    verifyAnswer(testDf.filter("(ID >= 2 OR NAME is null)"), expected4)
    verifyAnswer(testDf.filter("ID >= 2 OR NAME is null"), expected4)
    // simple And filter
    verifyAnswer(testDf.filter("ID < 4 and NAME is null"), Seq())

    // And/or filters.
    verifyAnswer(testDf.filter("(ID < 4 OR NAME is null) AND ID > 1"), expected4)
    verifyAnswer(testDf.filter("ID > 1 AND (ID < 4 OR NAME is null)"), expected4)
  }

  private def getTestdataframe(isTable: Boolean, numPartitions: Int): DataFrame = {
    val opts = if (isTable) {
      defaultOpts +
        ("dbtable" -> s"$tabName") +
        ("numPartitions" -> Integer.toString(numPartitions))
    } else {
      defaultOpts +
        ("query" -> s"select * from $tabName") +
        ("numPartitions" -> Integer.toString(1))
    }
    sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
  }
}
