package org.sens.core.expression.function
import org.sens.core.expression.SensExpression
import org.sens.parser.{ElementNotFoundException, ValidationContext, WrongFunctionArgumentsException}

import scala.util.{Failure, Success, Try}

case class FunctionReference(name: String) extends SensFunction {
  override def toSensString: String = name

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensFunction] = {
    if(!context.containsFunction(name))
      Failure(ElementNotFoundException(name))
    else
      Success(this)
  }

  override def validateArguments(arguments: List[SensExpression], context: ValidationContext): Try[Boolean] = {
    if(!context.containsFunction(name)) {
      Failure(ElementNotFoundException(name))
    } else {
      val funcDef = context.getFunction(name)
      if(funcDef.get.validateArguments(arguments)) {
        Success(true)
      } else {
        Failure(WrongFunctionArgumentsException(name, arguments.size))
      }
    }
  }

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    Some(this).filter(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    if (f(this)) List(this) else Nil
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      this
    }
}
