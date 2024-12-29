package org.sens.core.expression.literal
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class BooleanLiteral(value: Boolean) extends SensLiteral {
  override def toSensString: String = if (value) "true" else "false"
  override def stringValue: String = value.toString
}
