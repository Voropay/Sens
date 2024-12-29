package org.sens.core.expression.operation.logical

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.{SensBinaryOperation, SensOperation}
import org.sens.parser.ValidationContext

import scala.util.Try

case class And(operand1: SensExpression, operand2: SensExpression) extends SensOperation with SensBinaryOperation {
  override def toSensString: String = "(" + operand1.toSensString + " and " + operand2.toSensString  + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(And(
      operand1.validateAndRemoveVariablePlaceholders(context).get,
      operand2.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      And(
        operand1.replaceSubExpression(replaceSubExpression, withSubExpression),
        operand2.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
