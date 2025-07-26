package org.sens.core.expression.literal

import org.sens.core.expression.SensExpression
import org.sens.parser.WrongTypeException

trait SensTypeLiteral extends SensLiteral {

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensTypeLiteral =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case w: SensTypeLiteral => w
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensTypeLiteral")
      }
    } else {
      this
    }

  override def stringValue: String = toSensString
}
