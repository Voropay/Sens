package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.WrongTypeException

case class MapLiteral(items: Map[SensLiteral, SensLiteral]) extends SensLiteral {
  override def toSensString: String = {
    val entries = items.map(kv => kv._1.toSensString + ": " + kv._2.toSensString)
    "[" + entries.mkString(",\n") + "]"
  }

  override def getSubExpressions: List[SensExpression] = items.keys.toList ::: items.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
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

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      MapLiteral(
        items.map(item => {
          val key = item._1.replaceSubExpression(replaceSubExpression, withSubExpression) match {
            case literal: SensLiteral => literal
            case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensLiteral")
          }
          val value = item._2.replaceSubExpression(replaceSubExpression, withSubExpression) match {
            case literal: SensLiteral => literal
            case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensLiteral")
          }
          (key, value)
        })
      )
    }

  override def stringValue: String = items.map(item => item._1.stringValue + "_" + item._2.stringValue).mkString("_")
}
