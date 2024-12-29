package org.sens.core.expression

import org.sens.core.expression.operation.SensOperation
import org.sens.parser.ValidationContext

import scala.util.Try

case class Switch(conditions: Map[SensExpression, SensExpression], defaultValue: SensExpression) extends SensOperation {
  override def toSensString: String = {
    "(case " +
    conditions.map(
      kv =>
        "when " + kv._1.toSensString + " then " + kv._2.toSensString
    ).mkString(" ") +
    " else " + defaultValue.toSensString + ")"
  }

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(Switch(
      conditions.map(item =>
        item._1.validateAndRemoveVariablePlaceholders(context).get -> item._2.validateAndRemoveVariablePlaceholders(context).get),
      defaultValue.validateAndRemoveVariablePlaceholders(context).get
    ))
  }

  override def getSubExpressions: List[SensExpression] = defaultValue :: conditions.keys.toList ::: conditions.values.toList

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      defaultValue.findSubExpression(f)
        .orElse(collectFirstSubExpression(conditions.keys, f))
        .orElse(collectFirstSubExpression(conditions.values, f))

    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr :::
      conditions.keys.flatMap(_.findAllSubExpressions(f)).toList :::
      conditions.values.flatMap(_.findAllSubExpressions(f)).toList :::
      defaultValue.findAllSubExpressions(f)
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      val newDefaultValue = defaultValue.replaceSubExpression(replaceSubExpression, withSubExpression)
      val newConditions = conditions.map(item => (
        item._1.replaceSubExpression(replaceSubExpression, withSubExpression),
        item._2.replaceSubExpression(replaceSubExpression, withSubExpression)))
      Switch(newConditions, newDefaultValue)
    }
}
