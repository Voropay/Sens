package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.WrongTypeException

case class ListTypeLiteral(elementType: SensTypeLiteral) extends SensTypeLiteral {

  override def toSensString: String = "list<" + elementType.toSensString + ">"

  override def getSubExpressions: List[SensExpression] = elementType :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      elementType.findSubExpression(f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: elementType.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensTypeLiteral =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case stl: SensTypeLiteral => stl
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensTypeLiteral")
      }
    } else {
      ListTypeLiteral(
        elementType.replaceSubExpression(replaceSubExpression, withSubExpression)
      )
    }
}
