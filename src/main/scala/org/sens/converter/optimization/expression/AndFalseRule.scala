package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object AndFalseRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case And(BooleanLiteral(false), _) => true
      case And(_, BooleanLiteral(false)) => true
      case AndSeq(ops) if ops.contains(BooleanLiteral(false)) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case And(BooleanLiteral(false), _) => BooleanLiteral(false)
      case And(_, BooleanLiteral(false)) => BooleanLiteral(false)
      case AndSeq(ops) if ops.contains(BooleanLiteral(false)) => BooleanLiteral(false)
      case _ => expr
    }
  }

}
