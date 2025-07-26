package org.sens.core.expression.literal

case class FloatTypeLiteral() extends SensTypeLiteral {
  override def toSensString: String = "float"
}

