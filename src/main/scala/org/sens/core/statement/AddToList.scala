package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class AddToList(listName: SensExpression, value: SensExpression) extends SensStatement {
  override def toSensString: String = listName.toSensString + "[] = " + value.toSensString

  override def getSubExpressions: List[SensExpression] = listName :: value :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(AddToList(
      listName.validateAndRemoveVariablePlaceholders(context).get,
      value.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    listName.findSubExpression(f)
      .orElse(value.findSubExpression(f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    listName.findAllSubExpressions(f) ::: value.findAllSubExpressions(f)
  }


  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    AddToList(
      listName.replaceSubExpression(replaceSubExpression, withSubExpression),
      value.replaceSubExpression(replaceSubExpression, withSubExpression))
}
