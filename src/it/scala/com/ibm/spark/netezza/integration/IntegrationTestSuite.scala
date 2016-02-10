package com.ibm.spark.netezza.integration

import org.apache.spark.sql.DataFrame
import org.netezza.error.NzSQLException

class IntegrationTestSuite extends IntegrationSuiteBase with QueryTest{
  var testDf: DataFrame = _

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
             |charCol char
             |)
      """.stripMargin
        )

      // scalastyle:off
      conn.createStatement().executeUpdate(
        s"""
           |insert into $tableName values
           |(false, 2147483647, -128, 32767, 2147483648, 3.4, 5.6, 'a');
           |insert into $tableName values
           |(null, null, null, null, null, null, null, null)
           """.stripMargin
      )
      // scalastyle:on
    }

    dropTable(testTable)
    createTable(testTable)

    val opts = Map("url" -> testURL,
      "user" -> user,
      "password" -> password,
      "dbtable" -> testTable,
      "numPartitions" -> Integer.toString(numPartitions))
    testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
  }

  override def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

  test("Test load netezza to a DataFrame") {
    checkAnswer(testDf, TestUtils.expectedData) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}
