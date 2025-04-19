package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

trait SensAttribute extends SensElement {
  def getAttributeNames(context: ValidationContext): List[String]
  def getAttributes(context: ValidationContext): List[Attribute]

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensAttribute
  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensAttribute]
}
