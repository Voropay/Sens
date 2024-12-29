package org.sens.core.expression.operation.arithmetic

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.{SensOperation, SensUnaryOperation}
import org.sens.parser.ValidationContext

import scala.util.Try

case class UnaryMinus(operand: SensExpression) extends SensOperation with SensUnaryOperation {

  override def toSensString: String = "(" + operand.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(UnaryMinus(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      UnaryMinus(
        operand.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
