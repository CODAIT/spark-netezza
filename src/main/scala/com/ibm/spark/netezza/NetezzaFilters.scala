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

import java.sql.{Date, Timestamp}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources._

/**
 * Push down filters to the Netezza database. Generates where clause to inject into
 * select statement executed on the Netezza database. This implementation is based
 * on the spark sql builtin jdbc data source filters.
 */
private[netezza] object NetezzaFilters {

  /**
   * Returns a WHERE clause representing both `filters`, if any, and the current partition.
   */
  def getWhereClause(filters: Array[Filter], part: NetezzaPartition): String = {
    val filterWhereClause = getFilterClause(filters)
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  /**
   * Converts filters into a WHERE clause suitable for injection into a Netezza SQL query.
   */
  def getFilterClause(filters: Array[Filter]): String = {
    filters.flatMap(generateFilterExpr).map(p => s"($p)").mkString(" AND ")
  }

  /**
    * Converts value to SQL expression.
    */
  private def quoteValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeQuotes(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(quoteValue).mkString(", ")
    case _ => value
  }

  /**
   * Return a strings that escapes quote with another quote.
   */
  private def escapeQuotes(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
    * Generate Netezza SQL predicate expression for the input filter, if the filter can not
    * be handled, returns None.
    */
  def generateFilterExpr(f: Filter): Option[String] = {
    Option(f match {
      case EqualTo(attr, value) => s"$attr = ${quoteValue(value)}"
      case EqualNullSafe(attr, value) =>
        s"(NOT ($attr != ${quoteValue(value)} OR $attr IS NULL OR " +
          s"${quoteValue(value)} IS NULL) OR ($attr IS NULL AND ${quoteValue(value)} IS NULL))"
      case LessThan(attr, value) => s"$attr < ${quoteValue(value)}"
      case GreaterThan(attr, value) => s"$attr > ${quoteValue(value)}"
      case LessThanOrEqual(attr, value) => s"$attr <= ${quoteValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"$attr >= ${quoteValue(value)}"
      case IsNull(attr) => s"$attr IS NULL"
      case IsNotNull(attr) => s"$attr IS NOT NULL"
      case StringStartsWith(attr, value) => s"${attr} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${attr} LIKE '%${value}'"
      case StringContains(attr, value) => s"${attr} LIKE '%${value}%'"
      case In(attr, value) => s"$attr IN (${quoteValue(value)})"
      case Not(f) => generateFilterExpr(f).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        val or = Seq(f1, f2).flatMap(generateFilterExpr(_))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(generateFilterExpr(_))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }
}
