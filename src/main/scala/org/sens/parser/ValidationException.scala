package org.sens.parser

class ValidationException(reason: String) extends Exception(reason)

case class ParsingException(reason: String) extends ValidationException(
  "Error while parsing data model: %s"
    .format(reason)
)

case class ElementNotFoundException(name: String) extends ValidationException("Element %s doesn't exist".format(name))

case class WrongFunctionArgumentsException(functionName: String, actualArguments: Int)
  extends ValidationException(
    "Function %s called with wrong number of arguments %d"
      .format(functionName, actualArguments)
  )

case class WrongArgumentsException(elementName: String, actualArguments: Int)
  extends ValidationException(
    "Element %s created with wrong number of arguments %d"
      .format(elementName, actualArguments)
  )

case class WrongTypeException(elementName: String, expected: String, found: String)
  extends ValidationException(
    "Element %s called with wrong argument type: expected %s, found %s"
      .format(elementName, expected, found)
  )

case class WrongArgumentsTypeException(functionName: String, expected: String, found: String)
  extends ValidationException(
    "Function %s called with wrong argument type: expected %s, found %s"
      .format(functionName, expected, found)
  )

case class AttributeExpressionNotFound(name: String) extends ValidationException("Failed to infer attribute %s expression".format(name))

case class AttributeNamesDoNotMatch() extends ValidationException("Attribute names do not match")

case class RecursiveDefinitionNotAllowedException(name: String) extends ValidationException("Recursive definition now allowed: %s".format(name))

case class GenericDefinitionException(msg: String) extends ValidationException(msg)
