package org.sens.core.expression.operation.relational

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.{SensOperation, SensUnaryOperation}
import org.sens.core.expression.operation.comparison.SensComparison
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.Try

case class All(operand: SensComparison) extends SensOperation with SensUnaryOperation {
  override def toSensString: String = "(" + operand.operand1.toSensString + " " + operand.name + " all " + operand.operand2.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(All(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newOp = operand.replaceSubExpression(replaceSubExpression, withSubExpression)
      newOp match {
        case op: SensComparison => All(op)
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensComparison")
      }
    }
}