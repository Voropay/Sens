package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class If(condition: SensExpression, thenBlock: SensStatement, elseBlock: Option[SensStatement]) extends SensStatement {
  override def toSensString: String = 
    "if " + 
    condition.toSensString + 
    " then " +
    thenBlock.toSensString +
    (if (elseBlock.isDefined) " else " + elseBlock.get.toSensString else "")

  override def getSubExpressions: List[SensExpression] =
    condition :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(If(
      condition.validateAndRemoveVariablePlaceholders(context).get,
      thenBlock.validateAndRemoveVariablePlaceholders(context).get,
      elseBlock.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    val condSubExpression = condition.findSubExpression(f)
    if(condSubExpression.isDefined) {
      condSubExpression
    } else {
      val thenSubExpression = thenBlock.findSubExpression(f)
      if(thenSubExpression.isDefined) {
        thenSubExpression
      } else if(elseBlock.isDefined) {
        elseBlock.get.findSubExpression(f)
      } else {
        None
      }
    }
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    condition.findAllSubExpressions(f) :::
      thenBlock.findAllSubExpressions(f) :::
      elseBlock.toList.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement = {
    If(
      condition.replaceSubExpression(replaceSubExpression, withSubExpression),
      thenBlock.replaceSubExpression(replaceSubExpression, withSubExpression),
      elseBlock.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
  }
}
