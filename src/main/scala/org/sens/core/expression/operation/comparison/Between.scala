package org.sens.core.expression.operation.comparison

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.SensOperation
import org.sens.parser.ValidationContext

import scala.util.Try

case class Between(operand: SensExpression, leftLimit: SensExpression, rightLimit: SensExpression) extends SensOperation {
  override def toSensString: String = operand.toSensString + " between [" + leftLimit.toSensString + ", " + rightLimit.toSensString + "]"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(
      Between(
        operand.validateAndRemoveVariablePlaceholders(context).get,
        leftLimit.validateAndRemoveVariablePlaceholders(context).get,
        rightLimit.validateAndRemoveVariablePlaceholders(context).get)
    )
  }

  override def getSubExpressions: List[SensExpression] = operand :: leftLimit :: rightLimit :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      operand.findSubExpression(f)
        .orElse(leftLimit.findSubExpression(f))
        .orElse(rightLimit.findSubExpression(f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: operand.findAllSubExpressions(f) ::: leftLimit.findAllSubExpressions(f) ::: rightLimit.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      Between(
        operand.replaceSubExpression(replaceSubExpression, withSubExpression),
        leftLimit.replaceSubExpression(replaceSubExpression, withSubExpression),
        rightLimit.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
