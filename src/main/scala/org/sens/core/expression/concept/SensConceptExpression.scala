package org.sens.core.expression.concept

import org.sens.core.concept.SensConcept
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

trait SensConceptExpression extends SensConcept with SensExpression {
  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConceptExpression]
  override def inferAttributeExpressions(context: ValidationContext): Try[SensConceptExpression]
  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConceptExpression

  override def isTransparent: Boolean = false
}
