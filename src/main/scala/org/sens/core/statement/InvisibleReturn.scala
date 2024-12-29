package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class InvisibleReturn(value: Option[SensExpression]) extends SensStatement {
  override def toSensString: String = (if (value.isDefined) value.get.toSensString else "")

  override def getSubExpressions: List[SensExpression] = value.toList

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(InvisibleReturn(
      value.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (value.isEmpty) {
      None
    } else {
      value.get.findSubExpression(f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    value.toList.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    if (value.isEmpty) {
      this
    } else {
      InvisibleReturn(Some(value.get.replaceSubExpression(replaceSubExpression, withSubExpression)))
    }
}