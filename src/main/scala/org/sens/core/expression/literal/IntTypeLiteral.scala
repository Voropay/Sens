package org.sens.core.expression.literal

case class IntTypeLiteral() extends SensTypeLiteral {
  override def toSensString: String = "int"
}
