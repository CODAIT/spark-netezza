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

import org.apache.commons.csv.{CSVParser, CSVFormat}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Class provides methods to parse the data written by the Netezza into
  * the remote client pipe. Format of the data is controlled by the external
  * table definition options.
  */
class NetezzaRecordParser(delimiter: Char, escapeChar: Char, schema: StructType) {

  val csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape(escapeChar)
  val row: NetezzaRow = new NetezzaRow(schema)

  /**
    * Parse the input String into column values.
    *
    * @param input string value of a row
    * @return row object that contains the current values..
    */
  def parse(input: String): NetezzaRow = {
    val parser = CSVParser.parse(input, csvFormat)
    val records = parser.getRecords()
    records.isEmpty match {
      case true => {
        // null value for single column select.
        row.setValue(0, "")
      }
      case false => {
        // Parsing is one row at a tine , only one record expected.
        val record = records.get(0)
        for (i: Int <- 0 until record.size()) {
          row.setValue(i, record.get(i))
        }
      }
    }
    row
  }
}
