package org.sens.core

import org.sens.core.expression.SensExpression

trait SensElement {
  def toSensString: String
  def getSubExpressions: List[SensExpression]
  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression]
  def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression]
  def containsSubExpression(el: SensExpression): Boolean = findSubExpression(_ == el).isDefined
  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensElement

  protected def collectFirstSubExpression(args: Iterable[SensElement], f: SensExpression => Boolean): Option[SensExpression] = {
    for (arg <- args) {
      val res = arg.findSubExpression(f)
      if (res.isDefined) {
        return res
      }
    }
    None
  }
}
