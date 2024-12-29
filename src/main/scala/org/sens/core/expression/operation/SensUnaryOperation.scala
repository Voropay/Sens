package org.sens.core.expression.operation

import org.sens.core.expression.SensExpression

trait SensUnaryOperation extends SensOperation {
  val operand: SensExpression

  override def getSubExpressions: List[SensExpression] = operand :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      operand.findSubExpression(f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: operand.findAllSubExpressions(f)
  }

}
