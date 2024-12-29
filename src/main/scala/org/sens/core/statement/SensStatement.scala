package org.sens.core.statement

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

trait SensStatement extends SensElement {
  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement]
  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement
}
