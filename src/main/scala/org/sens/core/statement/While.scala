package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class While(condition: SensExpression, body: SensStatement) extends SensStatement {
  override def toSensString: String =
    "while " +
    condition.toSensString +
    " do " +
    body.toSensString

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(While(
      condition.validateAndRemoveVariablePlaceholders(context).get,
      body.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def getSubExpressions: List[SensExpression] = condition :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    condition.findSubExpression(f)
      .orElse(
        body.findSubExpression(f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    condition.findAllSubExpressions(f) ::: body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    While(
      condition.replaceSubExpression(replaceSubExpression, withSubExpression),
      body.replaceSubExpression(replaceSubExpression, withSubExpression))
}
