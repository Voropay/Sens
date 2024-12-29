package org.sens.converter.optimization.expression

import org.sens.core.expression.SensExpression

trait ExpressionOptimizationRule {
  def apply(expr: SensExpression): SensExpression = {
    var convertedExpr = expr
    var exprToReplace = convertedExpr.findSubExpression(findExpression)
    while (exprToReplace.isDefined) {
      val replaceWithExpr = modifyExpression(exprToReplace.get)
      convertedExpr = convertedExpr.replaceSubExpression(exprToReplace.get, replaceWithExpr)
      exprToReplace = convertedExpr.findSubExpression(findExpression)
    }
    convertedExpr
  }

  def findExpression(expr: SensExpression): Boolean

  def modifyExpression(expression: SensExpression): SensExpression
}
