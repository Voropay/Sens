package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.comparison._

object NotEqualsRule extends ExpressionOptimizationRule {
  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case Not(Equals(_, _)) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case Not(Equals(op1, op2)) => NotEquals(op1, op2)
      case _ => expr
    }
  }

}
