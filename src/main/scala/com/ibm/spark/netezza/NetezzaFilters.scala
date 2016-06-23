/**
  * (C) Copyright IBM Corp. 2015, 2016
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

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources._

/**
 * Push down filters to the Netezza database. Generates where clause to inject into
 * select statement executed on the Netezza database. This implementation is based
 * on the spark sql builtin jdbc data source.
 */
private[netezza] object NetezzaFilters {

  /**
   * Returns a WHERE clause representing both `filters`, if any, and the current partition.
   */
  def getWhereClause(filters: Array[Filter], part: NetezzaPartition): String = {
    val filterWhereClause = getFilterClause(filters)
    if (part.whereClause != null && filterWhereClause.length > 0) {
      filterWhereClause + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else {
      filterWhereClause
    }
  }

  /**
   * Converts filters into a WHERE clause suitable for injection into a Netezza SQL query.
   */
  def getFilterClause(filters: Array[Filter]): String = {
    val filterStrings = filters map generateFilterExpr filter (_ != null)
    if (filterStrings.size > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  /**
   * Convert the given String into a quotes SQL string value.
   */
  private def quoteValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeQuotes(stringValue)}'"
    case tsValue: Timestamp => s"'${tsValue}'"
    case _ => value
  }

  /**
   * Return a strings that escapes quote with another quote.
   */
  private def escapeQuotes(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Turns a single Filter into a String representing a SQL expression.
   * Returns null for an unhandled filter.
   */
  private def generateFilterExpr(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${quoteValue(value)}"
    case LessThan(attr, value) => s"$attr < ${quoteValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${quoteValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${quoteValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${quoteValue(value)}"
    case IsNull(attr) => s"$attr is null"
    case IsNotNull(attr) => s"$attr is not null"
    case _ => null
  }
}
