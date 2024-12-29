package org.sens.core.expression.operation.logical

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.{SensOperation, SensUnaryOperation}
import org.sens.parser.ValidationContext

import scala.util.Try

case class Not(operand: SensExpression) extends SensOperation with SensUnaryOperation {
  override def toSensString: String = "(not " + operand.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(Not(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      Not(operand.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}