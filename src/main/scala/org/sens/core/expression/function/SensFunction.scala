package org.sens.core.expression.function

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

trait SensFunction extends SensExpression {
  def validateArguments(arguments: List[SensExpression], context: ValidationContext): Try[Boolean]
  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensFunction]
}
