package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.WrongTypeException

case class MapTypeLiteral(elements: Map[String, SensTypeLiteral]) extends SensTypeLiteral {

  override def toSensString: String = {
    val entries = elements.map(kv => kv._2.toSensString + " " + kv._1)
    "map<" + entries.mkString(",\n") + ">"
  }

  override def getSubExpressions: List[SensExpression] = elements.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(elements.values, f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: elements.values.flatMap(_.findAllSubExpressions(f)).toList
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensTypeLiteral =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case stl: SensTypeLiteral => stl
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensTypeLiteral")
      }
    } else {
      MapTypeLiteral(
        elements.mapValues(_.replaceSubExpression(replaceSubExpression, withSubExpression))
      )
    }
}
