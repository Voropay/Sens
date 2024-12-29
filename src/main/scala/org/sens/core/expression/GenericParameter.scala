package org.sens.core.expression

import org.sens.core.expression.literal.SensLiteral
import org.sens.parser.{ElementNotFoundException, GenericDefinitionException, ValidationContext}

import scala.util.{Failure, Success, Try}

case class GenericParameter(name: String) extends SensExpression {
  override def toSensString: String = name

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = Some(this).filter(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    if (f(this)) List(this) else Nil
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      this
    }

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    if (!context.containsGenericParameter(name))
      Failure(ElementNotFoundException(name))
    else
      Success(this)
  }

  override def evaluate: Try[SensLiteral] = {
    Failure(GenericDefinitionException("Cannot evaluate generic parameter"))
  }
}
