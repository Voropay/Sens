package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class ForEachItemInList(listItem: String, list: SensExpression, body: SensStatement) extends SensStatement {
  override def toSensString: String = 
    "foreach(" +
    listItem + " in " +
    list.toSensString + ") " +
    body.toSensString

  override def getSubExpressions: List[SensExpression] = list :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    val newContext = context.addFrame
    newContext.addVariable(VariableDefinition(listItem, None))
    Try(ForEachItemInList(
      listItem,
      list.validateAndRemoveVariablePlaceholders(newContext).get,
      body.validateAndRemoveVariablePlaceholders(newContext).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    list.findSubExpression(f)
      .orElse(
        body.findSubExpression(f)
      )

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    list.findAllSubExpressions(f) ::: body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    ForEachItemInList(
      listItem,
      list.replaceSubExpression(replaceSubExpression, withSubExpression),
      body.replaceSubExpression(replaceSubExpression, withSubExpression))
}
