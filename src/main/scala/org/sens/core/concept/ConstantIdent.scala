package org.sens.core.concept

import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class ConstantIdent(name: String) extends SensIdent {
  override def getName: String = name

  override def toSensString: String = name

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ConstantIdent] = Success(this)

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensIdent = this
}
