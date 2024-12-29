package org.sens.core.statement

import org.sens.core.expression.{FunctionCall, SensExpression}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.Try

case class ProcedureCall(functionCall: FunctionCall) extends SensStatement {
  override def toSensString: String = functionCall.toSensString

  override def getSubExpressions: List[SensExpression] = functionCall :: Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    Try(ProcedureCall(
      functionCall.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    functionCall.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    functionCall.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement = {
    val newFunc = functionCall.replaceSubExpression(replaceSubExpression, withSubExpression)
    newFunc match {
      case func: FunctionCall => ProcedureCall(func)
      case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "FunctionCall")
    }
  }


}
