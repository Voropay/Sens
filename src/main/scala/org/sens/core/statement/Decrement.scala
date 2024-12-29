package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class Decrement(operand: SensExpression) extends SensStatement {

  override def toSensString: String = operand.toSensString + "--"

  override def getSubExpressions: List[SensExpression] = operand :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(Decrement(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    operand.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    operand.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    Decrement(operand.replaceSubExpression(replaceSubExpression, withSubExpression))
}
