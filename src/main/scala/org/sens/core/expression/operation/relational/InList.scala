package org.sens.core.expression.operation.relational

import org.sens.core.expression.{ListInitialization, SensExpression}
import org.sens.core.expression.operation.{SensBinaryOperation, SensOperation}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.Try

case class InList(operand1: SensExpression, operand2: ListInitialization) extends SensOperation with SensBinaryOperation {
  override def toSensString: String = "(" + operand1.toSensString + " in " + operand2.toSensString + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(InList(
      operand1.validateAndRemoveVariablePlaceholders(context).get,
      operand2.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newOp2 = operand2.replaceSubExpression(replaceSubExpression, withSubExpression)
      newOp2 match {
        case op2: ListInitialization => InList(operand1.replaceSubExpression(replaceSubExpression, withSubExpression), op2)
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "ListInitialization")
      }
    }

}