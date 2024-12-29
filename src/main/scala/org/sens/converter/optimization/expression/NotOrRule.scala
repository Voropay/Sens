package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.logical._

object NotOrRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case Not(Or(_, _)) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case Not(Or(op1, op2)) => AndSeq(Not(op1) :: Not(op2) :: Nil)
      case _ => expr
    }
  }

}
