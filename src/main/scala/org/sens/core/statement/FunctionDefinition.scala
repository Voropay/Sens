package org.sens.core.statement

import org.sens.core.DataModelElement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class FunctionDefinition (name: String, arguments: List[String], body: SensStatement) extends  SensFunctionDefinition with DataModelElement {
  override def toSensString: String = "def " + name + "(" + arguments.mkString(", ") + ") " + body.toSensString

  override def getSubExpressions: List[SensExpression] = Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    context.addFunction(this)
    val newContext = context.addFrame
    newContext.addVariables(arguments.map(VariableDefinition(_, None)))
    Try(FunctionDefinition(
      name,
      arguments,
      body.validateAndRemoveVariablePlaceholders(newContext).get
    ))
  }

  def validateArguments(currentArguments: List[SensExpression]): Boolean = arguments.size == currentArguments.size

  override def getName: String = name

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = body.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    FunctionDefinition(name, arguments, body.replaceSubExpression(replaceSubExpression, withSubExpression))
}