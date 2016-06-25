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

import org.apache.spark.sql.Row
import org.netezza.error.NzSQLException

/**
  * Test cases for reading data by specify SQL query.
  */
class QueryIntegrationTestSuite extends IntegrationSuiteBase {

  val tabName = "staff"
  val expected = Seq(
    Row(1, "John Doe"),
    Row(2, "Jeff Smith"),
    Row(3, "Kathy Saunders"),
    Row(4, null),
   Row(5, "abcd\nefg\thij\r\n\\"),
    Row(6, "klmn"))

  val expectedFiltered = Seq(Row(1, "John Doe"), Row(2, "Jeff Smith"))


  override def beforeAll(): Unit = {
    super.beforeAll()
    try {executeJdbcStmt(s"drop table $tabName")} catch { case e: NzSQLException => }
    executeJdbcStmt(s"create table $tabName(id int , name varchar(20))")
    executeJdbcStmt(s"insert into $tabName values(1 , 'John Doe')")
    executeJdbcStmt(s"insert into $tabName values(2 , 'Jeff Smith')")
    executeJdbcStmt(s"insert into $tabName values(3 , 'Kathy Saunders')")
    executeJdbcStmt(s"insert into $tabName values(4 , null)")
   executeJdbcStmt(s"insert into $tabName values(5 , 'abcd\nefg\thij\r\n\\')")
    executeJdbcStmt(s"insert into $tabName values(6 , 'klmn')")
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


  test("Test simple query with filter") {
      val opts = defaultOpts + ("query" -> s"select * from $tabName where id < 3")
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      verifyAnswer(testDf, expectedFiltered)
  }

  test("Test simple query with a data frame filter") {
    val opts = defaultOpts + ("query" -> s"select * from $tabName")
    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }

  test("Test simple query with partitions") {
    val opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(4)) +
      ("lowerbound" -> "1") +
      ("upperbound" -> "100")

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }

  test("Test simple query without specifying lower or upper boundary") {
    var opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(4))

    val testOpts = Seq(opts , opts + ("lowerbound" -> "1"), opts + ("upperbound" -> "10"))
    for (opts <- testOpts) {
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      verifyAnswer(testDf, expected)
      verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
    }
  }

  test("Test simple query with  single partition") {
    val opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(1))

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }

  test("Test query with number of partitions greated than the difference in boundary values.") {
    val opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(10))

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
    verifyAnswer(testDf.filter("ID < 3"), expectedFiltered)
  }


  test("Test Invalid number of partitions for query") {
    val opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("numPartitions" -> Integer.toString(4))

    val errMsg = intercept[IllegalArgumentException] {
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    }.getMessage

    assert(errMsg contains "Partition column should be specified or" +
      " number of partitions should be set to 1 with the query option.")
  }

  test("Test simple query with num partitions set to zero.") {
    val opts = defaultOpts +
      ("query" -> s"select * from $tabName") +
      ("partitioncol" -> "ID") +
      ("numPartitions" -> Integer.toString(0))

    val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
    verifyAnswer(testDf, expected)
  }
}
