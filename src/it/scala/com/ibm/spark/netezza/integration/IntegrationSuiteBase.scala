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

import java.sql.Connection

import com.ibm.spark.netezza.NetezzaJdbcUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait IntegrationSuiteBase extends FunSuite with BeforeAndAfterAll with QueryTest{
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var conn: Connection = _
  protected val prop = new java.util.Properties

  // Configurable vals
  protected var configFile = "application"
  protected var testURL: String = _
  protected var testTable: String = _
  protected var user: String = _
  protected var password: String = _
  protected var numPartitions: Int = _


  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = new SparkContext("local[*]", "IntegrationTest", new SparkConf())
    sqlContext = new SQLContext(sc)

    val conf = ConfigFactory.load(configFile)
    testURL = conf.getString("test.integration.dbURL")
    testTable = conf.getString("test.integration.table")
    user = conf.getString("test.integration.user")
    password = conf.getString("test.integration.password")
    numPartitions = conf.getInt("test.integration.partition.number")

    prop.setProperty("user", user)
    prop.setProperty("password", password)
    conn = NetezzaJdbcUtils.getConnector(testURL, prop)()
  }

  override def afterAll(): Unit = {
    try {
      sc.stop()
    }
    finally {
      conn.close()
      super.afterAll()
    }
  }

  /**
    * Executes the data frame and makes sure the answer matches the expected result.
    *
    * @param df             the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    */
  def verifyAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    checkAnswer(df, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }


  /**
    * Execute a JDBC statement.
    */
  def executeJdbcStmt(stmt: String) {
    conn.createStatement().executeUpdate(stmt)
  }

  /**
    * Drops table `tableName` after calling `f`.
    */
  def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        executeJdbcStmt(s"DROP TABLE $name")
      }
    }
  }
}
