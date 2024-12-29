package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.logical._

object AndTrueRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case And(BooleanLiteral(true), _) => true
      case And(_, BooleanLiteral(true)) => true
      case AndSeq(ops) if (ops.count(_ == BooleanLiteral(true)) >= ops.length - 1) => true
      case _ => false
    }
  }

  override def modifyExpression(expr: SensExpression): SensExpression = {
    expr match {
      case And(BooleanLiteral(true), op) => op
      case And(op, BooleanLiteral(true)) => op
      case AndSeq(ops) =>
        if(ops.count(_ == BooleanLiteral(true)) == ops.length - 1) {
          ops.find(_ != BooleanLiteral(true)).get
        } else if(ops.count(_ == BooleanLiteral(true)) == ops.length) {
          BooleanLiteral(true)
        } else {
          expr
        }
      case _ => expr
    }
  }

}
