package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object OrFalseRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case Or(BooleanLiteral(false), _) => true
      case Or(_, BooleanLiteral(false)) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case Or(BooleanLiteral(false), op) => op
      case Or(op, BooleanLiteral(false)) => op
      case _ => expr
    }
  }
}
