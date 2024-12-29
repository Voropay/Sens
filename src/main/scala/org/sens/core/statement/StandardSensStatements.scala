package org.sens.core.statement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

object StandardSensStatements {
  def functions: Map[String, SensFunctionDefinition] = Map(
    //String functions
    "concat" -> SensFunctionDefinitionWithArrayOfArguments("concat"),
    "like" -> SensFunctionDefinitionWithFixedNumberOfArguments("like", 2),
    "length" -> SensFunctionDefinitionWithFixedNumberOfArguments("length", 1),
    "upper" -> SensFunctionDefinitionWithFixedNumberOfArguments("upper", 1),
    "lower" -> SensFunctionDefinitionWithFixedNumberOfArguments("lower", 1),
    "substring" -> SensFunctionDefinitionWithVariableNumberOfArguments("substring", 2 :: 3 :: Nil),
    "replace" -> SensFunctionDefinitionWithFixedNumberOfArguments("replace", 3),
    //Casting
    "cast" -> SensFunctionDefinitionWithFixedNumberOfArguments("cast", 2),
    //Math functions
    "mod" -> SensFunctionDefinitionWithFixedNumberOfArguments("mod", 2),
    "round" -> SensFunctionDefinitionWithVariableNumberOfArguments("round", 1 :: 2 :: Nil),
    "ceil" -> SensFunctionDefinitionWithVariableNumberOfArguments("ceil", 1 :: 2 :: Nil),
    "floor" -> SensFunctionDefinitionWithVariableNumberOfArguments("floor", 1 :: 2 :: Nil),
    "trunc" -> SensFunctionDefinitionWithFixedNumberOfArguments("trunc", 1),
    "abs" -> SensFunctionDefinitionWithFixedNumberOfArguments("abs", 1),
    "sqrt" -> SensFunctionDefinitionWithFixedNumberOfArguments("sqrt", 1),
    //Other
    "coalesce" ->  SensFunctionDefinitionWithArrayOfArguments("coalesce"),
    //DateTime functions
    "current_time" -> SensFunctionDefinitionWithFixedNumberOfArguments("current_time", 0),
    "current_date" -> SensFunctionDefinitionWithFixedNumberOfArguments("current_date", 0),
    "current_timestamp" -> SensFunctionDefinitionWithFixedNumberOfArguments("current_timestamp", 0),
    //Aggregate functions
    "sum" -> SensFunctionDefinitionWithFixedNumberOfArguments("sum", 1),
    "count" -> SensFunctionDefinitionWithVariableNumberOfArguments("count", 0 :: 1 :: Nil),
    "max" -> SensFunctionDefinitionWithFixedNumberOfArguments("max", 1),
    "min" -> SensFunctionDefinitionWithFixedNumberOfArguments("min", 1),
    "avg" -> SensFunctionDefinitionWithFixedNumberOfArguments("avg", 1)
  )
}

case class SensFunctionDefinitionWithFixedNumberOfArguments(name: String, numberOfArguments: Int) extends SensFunctionDefinition {
  override def getName: String = name

  override def validateArguments(currentArguments: List[SensExpression]): Boolean = currentArguments.size == numberOfArguments

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = Success(this)

  override def toSensString: String = name

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensFunctionDefinition = this
}

case class SensFunctionDefinitionWithArrayOfArguments(name: String) extends SensFunctionDefinition {
  override def getName: String = name

  override def validateArguments(currentArguments: List[SensExpression]): Boolean = currentArguments.nonEmpty

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = Success(this)

  override def toSensString: String = name

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensFunctionDefinition = this
}

case class SensFunctionDefinitionWithVariableNumberOfArguments(name: String, arguments: List[Int]) extends SensFunctionDefinition {
  override def getName: String = name

  override def validateArguments(currentArguments: List[SensExpression]): Boolean = arguments.contains(currentArguments.size)

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = Success(this)

  override def toSensString: String = name

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensFunctionDefinition = this
}
