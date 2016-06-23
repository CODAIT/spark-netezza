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

import org.apache.spark.sql.{DataFrame, Row}
import org.netezza.error.NzSQLException

/**
  * Test case for fetching form table using column value based partition. Default is to use
  * the data slices for partitions.
  */
class TablePartitionColIntegrationTestSuite extends IntegrationSuiteBase with QueryTest {
  val tabName = "staff"
  val expected = Seq(
    Row(1, "John Doe"),
    Row(2, "Jeff Smith"),
    Row(3, "Kathy Saunders"),
    Row(4, null))

  val expectedFiltered = Seq(Row(1, "John Doe"), Row(2, "Jeff Smith"))


  override def beforeAll(): Unit = {
    super.beforeAll()
    try {executeJdbcStmt(s"drop table $tabName")} catch { case e: NzSQLException => }
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


  test("Test table read with column partitions") {
    val opts = defaultOpts +
      ("dbtable" -> s"$tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(4)) +
      ("lowerbound" -> "1") +
      ("upperbound" -> "100")

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }

  test("Test table read specifying lower or upper boundary") {
    var opts = defaultOpts +
      ("dbtable" -> s"$tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(4))

    val testOpts = Seq(opts , opts + ("lowerbound" -> "1"), opts + ("upperbound" -> "10"))
    for (opts <- testOpts) {
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      verifyAnswer(testDf, expected)
      verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
    }
  }

  test("Test table read with single partition") {
    val opts = defaultOpts +
      ("dbtable" -> s"$tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(1))

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }

  test("Test table with number of partitions set to zero.") {
    val opts = defaultOpts +
      ("dbtable" -> s"$tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(0))

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
  }
}
