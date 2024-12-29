package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression
import org.sens.core.expression.operation.logical.{And, AndSeq}

object AndSeqRule extends ExpressionOptimizationRule {

  override def findExpression(expr: SensExpression): Boolean = {
    expr match {
      case and: And => checkAndOperands(and)
      case andSeq: AndSeq => checkAndSeqOperands(andSeq)
      case _ => false
    }
  }

  def checkAndOperands(expr: And): Boolean =
    expr.operand1.isInstanceOf[And] || expr.operand2.isInstanceOf[And] || expr.operand1.isInstanceOf[AndSeq] || expr.operand2.isInstanceOf[AndSeq]

  def checkAndSeqOperands(expr: AndSeq): Boolean =
    expr.operands.exists(op => {
      op.isInstanceOf[And] || op.isInstanceOf[AndSeq]
    })

  override def modifyExpression(expression: SensExpression): SensExpression = {
    expression match {
      case and: And => AndSeq(getOperands(and.operand1) ::: getOperands(and.operand2))
      case andSeq: AndSeq => AndSeq(andSeq.operands.flatMap(getOperands))
      case _ => expression
    }
  }

  def getOperands(expr: SensExpression): List[SensExpression] =
    if (expr.isInstanceOf[And] || expr.isInstanceOf[AndSeq]) {
      expr.getSubExpressions
    } else {
      expr :: Nil
    }
}
