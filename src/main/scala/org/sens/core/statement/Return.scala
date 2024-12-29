package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class Return(value: Option[SensExpression]) extends SensStatement {
  override def toSensString: String = "return" + (if (value.isDefined) " " + value.get.toSensString else "")

  override def getSubExpressions: List[SensExpression] = value.toList

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(Return(
      value.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    if (value.isEmpty) {
      None
    } else {
      value.get.findSubExpression(f)
    }
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    value.toList.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement = {
    if (value.isEmpty) {
      this
    } else {
      Return(Some(value.get.replaceSubExpression(replaceSubExpression, withSubExpression)))
    }
  }
}
