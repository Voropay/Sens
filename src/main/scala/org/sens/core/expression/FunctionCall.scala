package org.sens.core.expression

import org.sens.core.expression.function.{AnonymousFunctionDefinition, SensFunction}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.{Failure, Try}

case class FunctionCall(definition: SensFunction, arguments: List[SensExpression]) extends SensExpression {
  override def toSensString: String =
    (definition match {
      case x: AnonymousFunctionDefinition => "(" + x.toSensString + ")"
      case x: SensFunction => x.toSensString
    }) +
      "(" + arguments.map(_.toSensString).mkString(", ") + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[FunctionCall] = {
    val funcNumberOfArguments = definition.validateArguments(arguments, context)
    if(funcNumberOfArguments.isFailure) {
      Failure(funcNumberOfArguments.failed.get)
    } else
      Try(FunctionCall(
        definition.validateAndRemoveVariablePlaceholders(context).get,
        arguments.map(_.validateAndRemoveVariablePlaceholders(context).get)
      ))
  }

  override def getSubExpressions: List[SensExpression] = definition :: arguments

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      definition.findSubExpression(f)
        .orElse(collectFirstSubExpression(arguments, f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: definition.findAllSubExpressions(f) ::: arguments.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newDefinition = definition.replaceSubExpression(replaceSubExpression, withSubExpression)
      val newArguments = arguments.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
      newDefinition match {
        case func: SensFunction => FunctionCall(func, newArguments)
        case other => throw WrongTypeException("FunctionCall", other.getClass.getTypeName, "SensFunction")
      }

    }

}
