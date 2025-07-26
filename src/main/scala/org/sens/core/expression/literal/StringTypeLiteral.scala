package org.sens.core.expression.literal

case class StringTypeLiteral(length: Int) extends SensTypeLiteral {

  override def toSensString: String = "string(" + length + ")"
}
