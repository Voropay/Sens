package org.sens.core.expression.operation.relational

import org.sens.core.expression.SensExpression
import org.sens.core.expression.concept.SensConceptExpression
import org.sens.core.expression.operation.{SensOperation, SensUnaryOperation}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.Try

case class Exists(operand: SensConceptExpression) extends SensOperation with SensUnaryOperation {
  override def toSensString: String = "exists " + operand.toSensString

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(Exists(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newOp = operand.replaceSubExpression(replaceSubExpression, withSubExpression)
      newOp match {
        case op: SensConceptExpression => Exists(op)
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensConceptExpression")
      }
    }
}
