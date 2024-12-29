package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object OrTrueRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case Or(BooleanLiteral(true), _) => true
      case Or(_, BooleanLiteral(true)) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case Or(BooleanLiteral(true), _) => BooleanLiteral(true)
      case Or(_, BooleanLiteral(true)) => BooleanLiteral(true)
      case _ => expr
    }
  }
}
