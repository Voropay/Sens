package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object AndANotARule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case And(op1, Not(op2)) if (op1 == op2) => true
      case And(Not(op1), op2) if (op1 == op2) => true
      case AndSeq(ops) if containsAAndNotA(ops) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case And(op1, Not(op2)) if (op1 == op2)  => BooleanLiteral(false)
      case And(Not(op1), op2) if (op1 == op2) => BooleanLiteral(false)
      case AndSeq(ops) if containsAAndNotA(ops) => BooleanLiteral(false)
      case _ => expr
    }
  }

  def containsAAndNotA(ops: List[SensExpression]): Boolean = {
    ops.exists(el => ops.contains(Not(el)))
  }

}
