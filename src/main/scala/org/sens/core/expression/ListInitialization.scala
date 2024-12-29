package org.sens.core.expression

import org.sens.parser.ValidationContext

import scala.util.Try

case class ListInitialization(items: List[SensExpression]) extends SensExpression {
  override def toSensString: String = "[" + items.map(_.toSensString).mkString(", ") + "]"

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ListInitialization] = {
    Try(ListInitialization(
      items.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  override def getSubExpressions: List[SensExpression] = items

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(items, f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: items.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newItems = items.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
      ListInitialization(newItems)
    }
}
