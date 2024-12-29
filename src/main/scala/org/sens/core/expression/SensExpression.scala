package org.sens.core.expression

import org.sens.core.SensElement
import org.sens.core.expression.literal.SensLiteral
import org.sens.parser.ValidationContext

import scala.util.{Try, Failure}

trait SensExpression extends SensElement {
  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression]
  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression
  def evaluate: Try[SensLiteral] = Failure(new NotImplementedError())

}
