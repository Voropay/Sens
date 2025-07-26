package org.sens.core.expression.literal

case class BooleanTypeLiteral() extends SensTypeLiteral {
  override def toSensString: String = "boolean"
}

