package org.sens.core.expression

import org.sens.core.expression.operation.SensOperation
import org.sens.parser.ValidationContext

import scala.util.Try

case class If(condition: SensExpression, operandThen: SensExpression, operandElse: SensExpression) extends SensOperation {
  override def toSensString: String = "(if " + condition.toSensString + " then " + operandThen.toSensString + " else " + operandElse.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(If(
      condition.validateAndRemoveVariablePlaceholders(context).get,
      operandThen.validateAndRemoveVariablePlaceholders(context).get,
      operandElse.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def getSubExpressions: List[SensExpression] = condition :: operandThen :: operandElse :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      condition.findSubExpression(f)
        .orElse(operandThen.findSubExpression(f))
        .orElse(operandElse.findSubExpression(f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: condition.findAllSubExpressions(f) ::: operandThen.findAllSubExpressions(f) ::: operandElse.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      If(
        condition.replaceSubExpression(replaceSubExpression, withSubExpression),
        operandThen.replaceSubExpression(replaceSubExpression, withSubExpression),
        operandElse.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
