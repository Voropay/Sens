package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object OrANotARule extends ExpressionOptimizationRule {
  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case Or(op1, Not(op2)) if (op1 == op2) => true
      case Or(Not(op1), op2) if (op1 == op2) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case Or(op1, Not(op2)) if (op1 == op2) => BooleanLiteral(true)
      case Or(Not(op1), op2) if (op1 == op2) => BooleanLiteral(true)
      case _ => expr
    }
  }
}
