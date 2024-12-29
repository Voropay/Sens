package org.sens.core.statement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class NOP() extends SensStatement {
  override def toSensString: String = ""

  override def getSubExpressions: List[SensExpression] = Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = Success(this)

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement = this
}
