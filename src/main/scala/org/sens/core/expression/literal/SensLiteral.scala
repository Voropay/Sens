package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

trait SensLiteral extends SensExpression {
  override def getSubExpressions: List[SensExpression] = Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = Success(this)

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    Some(this).filter(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    if (f(this)) List(this) else Nil
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      this
    }

  override def evaluate: Try[SensLiteral] = Success(this)

  def stringValue: String
}
