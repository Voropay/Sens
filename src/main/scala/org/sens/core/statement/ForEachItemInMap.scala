package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class ForEachItemInMap(itemKey: String, itemValue: String, map: SensExpression, body: SensStatement) extends SensStatement {
  override def toSensString: String =
    "foreach(" +
    itemKey + ", " +
    itemValue + " in " +
    map.toSensString + ") " +
    body.toSensString

  override def getSubExpressions: List[SensExpression] = map :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    val newContext = context.addFrame
    newContext.addVariable(VariableDefinition(itemKey, None))
    newContext.addVariable(VariableDefinition(itemValue, None))
    Try(ForEachItemInMap(
      itemKey,
      itemValue,
      map.validateAndRemoveVariablePlaceholders(newContext).get,
      body.validateAndRemoveVariablePlaceholders(newContext).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    map.findSubExpression(f)
      .orElse(
        body.findSubExpression(f)
      )

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    map.findAllSubExpressions(f) ::: body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    ForEachItemInMap(
      itemKey,
      itemValue,
      map.replaceSubExpression(replaceSubExpression, withSubExpression),
      body.replaceSubExpression(replaceSubExpression, withSubExpression))
}
