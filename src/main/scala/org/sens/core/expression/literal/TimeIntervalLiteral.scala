package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.TimeIntervalLiteral.TimeUnit
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

class TimeIntervalLiteral(
                           value: Int,
                           leadingTimeUnit: TimeUnit,
                           leadingPrecision: Int,
                           trailingTimeUnit: TimeUnit,
                           trailingPrecision: Int
                         ) extends SensLiteral {
  def getValue: Int = value
  def getLeadingTimeUnit: TimeUnit = leadingTimeUnit
  def getLeadingPrecision: Int = leadingPrecision
  def getTrailingTimeUnit: TimeUnit = trailingTimeUnit
  def getTrailingPrecision: Int = trailingPrecision

  override def toSensString: String = "INTERVAL " +
    "'" + value + "' " +
    leadingTimeUnit +
    (if(leadingPrecision > 0) "(" + leadingPrecision + ")" else "") +
    (if(trailingTimeUnit != null) " TO " + trailingTimeUnit else "") +
    (if(trailingTimeUnit != null && trailingPrecision > 0) "(" + trailingPrecision + ")" else "")

  override def stringValue: String = value.toString + "_" + leadingTimeUnit.toString +
    (if (leadingPrecision > 0) "_" + leadingPrecision.toString else "") +
    (if (trailingTimeUnit != null) " _ " + trailingTimeUnit.toString else "") +
    (if (trailingTimeUnit != null && trailingPrecision > 0) "_" + trailingPrecision.toString else "")
}

object TimeIntervalLiteral extends Enumeration {
  type TimeUnit = Value
  val YEAR, MONTH, DAY, HOUR, MINUTE, SECOND = Value

  def apply(value: Int, leadingTimeUnit: TimeUnit, trailingTimeUnit: TimeUnit): TimeIntervalLiteral = {
    new TimeIntervalLiteral(value, leadingTimeUnit, 0, trailingTimeUnit, 0)
  }

  def apply(value: Int, leadingTimeUnit: TimeUnit): TimeIntervalLiteral = {
    new TimeIntervalLiteral(value, leadingTimeUnit, 0, null, 0)
  }

  def apply(value: Int, leadingTimeUnit: TimeUnit, leadingPrecision: Int): TimeIntervalLiteral = {
    new TimeIntervalLiteral(value, leadingTimeUnit, leadingPrecision, null, 0)
  }

  def apply(value: Int, leadingTimeUnit: TimeUnit, leadingPrecision: Int, trailingTimeUnit: TimeUnit): TimeIntervalLiteral = {
    new TimeIntervalLiteral(value, leadingTimeUnit, leadingPrecision, trailingTimeUnit, 0)
  }

  def apply(value: Int, leadingTimeUnit: TimeUnit, leadingPrecision: Int, trailingTimeUnit: TimeUnit, trailingPrecision: Int): TimeIntervalLiteral = {
    new TimeIntervalLiteral(value, leadingTimeUnit, leadingPrecision, trailingTimeUnit, trailingPrecision)
  }
}
