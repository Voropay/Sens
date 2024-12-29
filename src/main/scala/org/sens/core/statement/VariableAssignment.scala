package org.sens.core.statement

import org.sens.core.expression.SensExpression
import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.{Failure, Success, Try}

case class VariableAssignment(name: String, value: SensExpression) extends SensStatement {
  override def toSensString: String = name + " = " + value.toSensString

  override def getSubExpressions: List[SensExpression] = value :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    if(!context.containsVariable(name))
      Failure(ElementNotFoundException(name))
    else {
      Try(VariableAssignment(
        name,
        value.validateAndRemoveVariablePlaceholders(context).get
      ))
    }
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    value.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    value.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    VariableAssignment(name, value.replaceSubExpression(replaceSubExpression, withSubExpression))
}
