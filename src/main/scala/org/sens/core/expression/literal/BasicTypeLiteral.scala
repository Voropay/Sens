package org.sens.core.expression.literal

import org.sens.core.expression.literal.SensBasicTypes.SensBasicType

case class BasicTypeLiteral(value: SensBasicType) extends SensLiteral {
  override def toSensString: String = value.toString

  override def stringValue: String = value.toString
}

object SensBasicTypes extends Enumeration {
  type SensBasicType = Value
  val BOOLEAN_TYPE = Value("boolean")
  val FLOAT_TYPE = Value("float")
  val INT_TYPE = Value("int")
  val STRING_TYPE = Value("string")
}
