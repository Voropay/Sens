package org.sens.core.expression.operation.comparison

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.{SensBinaryOperation, SensOperation}
import org.sens.parser.ValidationContext

import scala.util.Try

case class GreaterOrEqualsThan(operand1: SensExpression, operand2: SensExpression) extends SensComparison with SensBinaryOperation  {
  override def name: String = ">="

  override def toSensString: String = "(" + operand1.toSensString + " >= " + operand2.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensComparison] = {
    Try(GreaterOrEqualsThan(
      operand1.validateAndRemoveVariablePlaceholders(context).get,
      operand2.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      GreaterOrEqualsThan(
        operand1.replaceSubExpression(replaceSubExpression, withSubExpression),
        operand2.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}