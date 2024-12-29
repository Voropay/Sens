package org.sens.core.expression.operation.arithmetic

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.SensBinaryOperation
import org.sens.parser.ValidationContext

import scala.util.Try

case class Substract(operand1: SensExpression, operand2: SensExpression) extends SensBinaryOperation {
  override def toSensString: String = "(" + operand1.toSensString + " - " + operand2.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(Substract(
      operand1.validateAndRemoveVariablePlaceholders(context).get,
      operand2.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      Substract(
        operand1.replaceSubExpression(replaceSubExpression, withSubExpression),
        operand2.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
