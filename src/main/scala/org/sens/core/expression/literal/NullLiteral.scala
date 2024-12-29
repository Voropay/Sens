package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class NullLiteral() extends SensLiteral {
  override def toSensString: String = "Null"
  override def stringValue: String = "Null"
}
