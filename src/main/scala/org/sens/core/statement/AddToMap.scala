package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class AddToMap(mapName: SensExpression, key: SensExpression, value: SensExpression) extends SensStatement {
  override def toSensString: String = mapName.toSensString + "[" + key.toSensString + "] = " + value.toSensString

  override def getSubExpressions: List[SensExpression] = mapName :: key :: value :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(AddToMap(
      mapName.validateAndRemoveVariablePlaceholders(context).get,
      key.validateAndRemoveVariablePlaceholders(context).get,
      value.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    mapName.findSubExpression(f)
      .orElse(key.findSubExpression(f))
      .orElse(value.findSubExpression(f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    mapName.findAllSubExpressions(f) ::: key.findAllSubExpressions(f) ::: value.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    AddToMap(
      mapName.replaceSubExpression(replaceSubExpression, withSubExpression),
      key.replaceSubExpression(replaceSubExpression, withSubExpression),
      value.replaceSubExpression(replaceSubExpression, withSubExpression)
    )
}
