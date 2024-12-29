package org.sens.core.expression

import org.sens.parser.ValidationContext

import scala.util.Try

case class MapInitialization(items: Map[SensExpression, SensExpression]) extends SensExpression {
  override def toSensString: String = {
    val entries = items.map(kv => kv._1.toSensString + ": " + kv._2.toSensString)
    "[" + entries.mkString(", ") + "]"
  }

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(MapInitialization(
      items.map(item =>
        item._1.validateAndRemoveVariablePlaceholders(context).get -> item._2.validateAndRemoveVariablePlaceholders(context).get
      )
    ))
  }

  override def getSubExpressions: List[SensExpression] = items.keys.toList ::: items.values.toList

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(items.keys, f)
        .orElse(collectFirstSubExpression(items.values, f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: items.keys.flatMap(_.findAllSubExpressions(f)).toList ::: items.values.flatMap(_.findAllSubExpressions(f)).toList
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newItems = items.map(item => (
        item._1.replaceSubExpression(replaceSubExpression, withSubExpression),
        item._2.replaceSubExpression(replaceSubExpression, withSubExpression)))
      MapInitialization(newItems)
    }
}
