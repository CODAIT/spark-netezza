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

import org.apache.spark.sql.Row

object TestUtils {
  /**
    * Expected output corresponding to the output of integration test.
    */
  val expectedData: Seq[Row] = Seq(
      Row(false, 2147483647, -128, 32767, 2147483648L, 3.4, 5.6, "a", "rAnD0m 5Tring",
        new Timestamp(0)),
      Row(List.fill(10)(null): _*))
}
