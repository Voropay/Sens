package org.sens.core.expression.operation

import org.sens.core.expression.SensExpression

trait SensBinaryOperation extends SensOperation {
  val operand1: SensExpression
  val operand2: SensExpression

  override def getSubExpressions: List[SensExpression] = operand1 :: operand2 :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      operand1.findSubExpression(f)
        .orElse(operand2.findSubExpression(f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: operand1.findAllSubExpressions(f) ::: operand2.findAllSubExpressions(f)
  }

}
