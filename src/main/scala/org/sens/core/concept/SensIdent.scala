package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

trait SensIdent extends SensElement {
  def getName: String
  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensIdent
  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensIdent]
}
