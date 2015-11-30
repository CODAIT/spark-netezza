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

package com.ibm.spark.netezza

import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.scalatest.FunSuite

/**
 * Helper class for common methods to write unit tests.
 */
abstract class NetezzaBaseSuite extends FunSuite {

  case class Column(name: String, jdbcType: Int,
                    precision: Int = 0, scale: Int = 0, signed: Boolean = false)

  /**
   * Utility routine to build spark sqk schema from jdbc meta data for tesing.
   * @param cols  input column metadata
   * @return spark sqk sql schema corresponding to the input jdbc metadata.
   */
  def buildSchema(cols: Array[Column]): StructType = {
    val fields = new Array[StructField](cols.length)
    var i = 0
    for (col <- cols) {
      val columnType = NetezzaSchema.getSparkSqlType(
        col.jdbcType, col.precision, col.scale, col.signed)
      val metadata = new MetadataBuilder().putString("name", col.name)
      fields(i) = StructField(col.name, columnType, true, metadata.build())
      i = i + 1
    }
    new StructType(fields)
  }
}
