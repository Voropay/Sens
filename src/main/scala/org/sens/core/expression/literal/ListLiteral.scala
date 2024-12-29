package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.WrongTypeException

case class ListLiteral(items: List[SensLiteral]) extends SensLiteral {
  
  override def toSensString: String = "[" + items.map(_.toSensString).mkString(", ") + "]"

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
      ListLiteral(
        items.map(item => {
         item.replaceSubExpression(replaceSubExpression, withSubExpression) match {
           case literal: SensLiteral => literal
           case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensLiteral")
         }
        })
      )
    }

  override def stringValue: String = items.map(_.stringValue).mkString("_")

}
