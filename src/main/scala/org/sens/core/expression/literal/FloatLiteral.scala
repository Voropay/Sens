package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class FloatLiteral(value: Double) extends SensLiteral {
  override def toSensString: String = value.toString
  override def stringValue: String = value.toString
}
