package com.ibm.spark.netezza.integration

import org.apache.spark.sql.Row

object TestUtils {
  /**
    * Expected output corresponding to the output of integration test.
    */
  val expectedData: Seq[Row] = Seq(
      Row(false, 2147483647, -128, 32767, 2147483648L, 3.4, 5.6, "a"),
      Row(List.fill(8)(null): _*))
}
