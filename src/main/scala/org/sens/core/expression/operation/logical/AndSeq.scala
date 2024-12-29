package org.sens.core.expression.operation.logical

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.SensOperation
import org.sens.parser.ValidationContext

import scala.util.Try

case class AndSeq(operands: List[SensExpression]) extends SensOperation {
  override def toSensString: String = "(" + operands.map(_.toSensString).mkString(" and ") + ")"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(AndSeq(
      operands.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def getSubExpressions: List[SensExpression] = operands

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(operands, f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: operands.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      AndSeq(operands.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)))
    }

}
