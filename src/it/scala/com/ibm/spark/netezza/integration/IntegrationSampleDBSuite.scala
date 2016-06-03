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

import com.ibm.spark.netezza.NetezzaJdbcUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.netezza.error.NzSQLException
import org.slf4j.LoggerFactory

/**
  * This test reads data from an existing database using this data source
  * library and compares the count againest source database.
  */
class IntegrationSampleDBSuite extends IntegrationSuiteBase with QueryTest {
  configFile = "sampledb"

  private val log = LoggerFactory.getLogger(getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()
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

  test("Fetch data from all the tables in the sample database.") {
    getTables().foreach { tableName =>
      log.info(s"Testing table: $tableName")
      val quoutedTableName = s""""$tableName""""
      val opts = defaultOpts() + ("dbTable" -> quoutedTableName)
      val testDf = sqlContext.read.format("com.ibm.spark.netezza").options(opts).load()
      val expectedCount = getCount(quoutedTableName)
      val dfCount = testDf.collect().count(p => true)
      assert(expectedCount == dfCount, s"Count does not match for table '$tableName'")
      // testDf.show(expectedCount)

    }
  }

  /**
    * Return all table names in the sample database.
    */
  private def getTables() : Seq[String] = {
    val dbMeta = conn.getMetaData()
    val rs = dbMeta.getTables(null, null , null, Array("TABLE" , "VIEW"))
    var tables = Seq[String]()
    while (rs.next()) {
      tables = tables :+ rs.getString("TABLE_NAME")
    }
    rs.close()
    tables
  }

  private def getCount(tableName: String) = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"select count(*) from $tableName")
    val count = if (rs.next()) {
      val count = rs.getInt(1)
      rs.close()
      stmt.close()
      count
    } else {-1}
    count
  }
}
