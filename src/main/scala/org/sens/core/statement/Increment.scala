package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class Increment(operand: SensExpression) extends SensStatement {
  override def toSensString: String = operand.toSensString + "++"

  override def getSubExpressions: List[SensExpression] = operand :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(Increment(
      operand.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    operand.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    operand.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    Increment(operand.replaceSubExpression(replaceSubExpression, withSubExpression))

}
