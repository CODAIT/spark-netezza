package com.ibm.spark.netezza.integration

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite
/**
  * A copy of subset of Spark SQL's `QueryTest` trait.
  */
trait QueryTest extends FunSuite {
  /**
    * Runs the plan and makes sure the answer matches the expected result.
    * If there was exception during the execution or the contents of the DataFrame does not
    * match the expected result, an error message will be returned. Otherwise, a [[None]] will
    * be returned.
    * @param df the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    val isSorted = df.queryExecution.logical.collect { case s: logical.Sort => s }.nonEmpty

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def sameRows(
                expectedAnswer: Seq[Row],
                sparkAnswer: Seq[Row],
                isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
      val errorMessage =
        s"""
           |== Results ==
           |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
        """.stripMargin
      return Some(errorMessage)
    }
    None
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.size).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.size) + 3)) + r
    }
  }
}
