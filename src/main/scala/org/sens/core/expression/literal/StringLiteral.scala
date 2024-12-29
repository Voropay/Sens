package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class StringLiteral(value: String) extends SensLiteral {
  override def toSensString: String = "\"" + value + "\""
  override def stringValue: String = value
}
