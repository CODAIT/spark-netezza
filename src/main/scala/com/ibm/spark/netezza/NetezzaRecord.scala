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

import java.text.SimpleDateFormat

/**
  * Netezza record holds one row from the netezza table. Provides methods to convert
  * from Strings into the data frame data type.
  */
class NetezzaRecord(row: Array[String]) {

  // Suresh temp method for testing ..
  def getNetezzaValues(): Array[String] = {
    row
  }
}
