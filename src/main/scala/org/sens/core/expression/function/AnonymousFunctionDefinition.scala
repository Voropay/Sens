package org.sens.core.expression.function

import org.sens.core.expression.SensExpression
import org.sens.core.statement.{SensStatement, VariableDefinition}
import org.sens.parser.{ValidationContext, WrongFunctionArgumentsException}

import scala.util.{Failure, Success, Try}

case class AnonymousFunctionDefinition(arguments: List[String], body: SensStatement) extends SensFunction {
  override def toSensString: String = "(" + arguments.mkString(", ") + ") => " + body.toSensString

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensFunction] = {
    val newContext = context.addFrame
    newContext.addVariables(arguments.map(VariableDefinition(_, None)))
    Try(AnonymousFunctionDefinition(
      arguments,
      body.validateAndRemoveVariablePlaceholders(newContext).get
    ))
  }

  override def validateArguments(currentArguments: List[SensExpression], context: ValidationContext): Try[Boolean] =
    if(currentArguments.size == arguments.size) {
      Success(true)
    } else {
      Failure(WrongFunctionArgumentsException("(" + arguments.mkString(", ") + ")", arguments.size))
    }

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      body.findSubExpression(f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      AnonymousFunctionDefinition(
        arguments,
        body.replaceSubExpression(replaceSubExpression, withSubExpression))
    }
}
