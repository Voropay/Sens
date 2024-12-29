package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class VariableDefinition(name: String, value: Option[SensExpression]) extends SensStatement {
  override def toSensString: String = "var " + name + (if (value.isDefined) " = " + value.get.toSensString else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    context.addVariable(this)
    Try(VariableDefinition(
      name,
      value.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def getSubExpressions: List[SensExpression] = value.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    if(value.isEmpty) {
      None
    } else {
      value.get.findSubExpression(f)
    }
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    value.toList.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    if (value.isEmpty) {
      this
    } else {
      VariableDefinition(name, Some(value.get.replaceSubExpression(replaceSubExpression, withSubExpression)))
    }
}
