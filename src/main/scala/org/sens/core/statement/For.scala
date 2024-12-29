package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class For(startOperator: Option[SensStatement], condition: Option[SensExpression], endOperator: Option[SensStatement], body: SensStatement) extends SensStatement {
  override def toSensString: String =
    "for(" +
    (if (startOperator.isDefined) {startOperator.get.toSensString} else "") + "; " +
    (if (condition.isDefined) {condition.get.toSensString} else "") + "; " +
    (if (endOperator.isDefined) {endOperator.get.toSensString} else "") + ") " +
    body.toSensString

  override def getSubExpressions: List[SensExpression] =
    condition.toList

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    val newContext = context.addFrame
    Try(For(
      startOperator.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      condition.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      endOperator.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      body.validateAndRemoveVariablePlaceholders(newContext).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    val startSubExpression = startOperator.flatMap(_.findSubExpression(f))
    if(startSubExpression.isDefined) {
      startSubExpression
    } else {
      val conditionSubExpression = condition.flatMap(_.findSubExpression(f))
      if(conditionSubExpression.isDefined) {
        conditionSubExpression
      } else {
        val endSubExpression = endOperator.flatMap(_.findSubExpression(f))
        if (endSubExpression.isDefined) {
          endSubExpression
        } else {
          body.findSubExpression(f)
        }
      }
    }
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    startOperator.toList.flatMap(_.findAllSubExpressions(f)) :::
      condition.toList.flatMap(_.findAllSubExpressions(f)) :::
      endOperator.toList.flatMap(_.findAllSubExpressions(f)) :::
      body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    For(
      startOperator.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      condition.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      endOperator.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      body.replaceSubExpression(replaceSubExpression, withSubExpression)
    )

}
