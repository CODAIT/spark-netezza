package com.ibm.spark.netezza.integration

import java.sql.Connection

import com.ibm.spark.netezza.NetezzaJdbcUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait IntegrationSuiteBase extends FunSuite with BeforeAndAfterAll{
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var conn: Connection = _
  protected val prop = new java.util.Properties

  // Configurable vals
  protected var testURL: String = _
  protected var testTable: String = _
  protected var user: String = _
  protected var password: String = _
  protected var numPartitions: Int = _


  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = new SparkContext("local[*]", "IntegrationTest")
    sqlContext = new SQLContext(sc)

    val conf = ConfigFactory.load
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
}
