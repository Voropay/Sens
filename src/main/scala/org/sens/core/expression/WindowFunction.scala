package org.sens.core.expression

import org.sens.core.concept.Order
import org.sens.parser.{ValidationContext, ValidationException, WrongFunctionArgumentsException}

import scala.util.{Failure, Try}

case class WindowFunction(
                           functionName: WindowFunctions.Name,
                           arguments: List[SensExpression],
                           partitionBy: List[SensExpression],
                           orderBy: List[Order],
                           rowsBetween: (Option[SensExpression],Option[SensExpression]),
                           rangeBetween: (Option[SensExpression],Option[SensExpression])
                         ) extends SensExpression {

  override def toSensString: String =
    functionName.toString +
      "(" + arguments.map(_.toSensString).mkString(", ") + ") over (\n" +
      (if(partitionBy.nonEmpty) {"partition by (" + partitionBy.map(_.toSensString).mkString(", ") + ")\n"} else {""}) +
      (if(orderBy.nonEmpty) {"order by (" + orderBy.map(_.toSensString).mkString(", ") + ")\n"} else {""}) +
      (if(rowsBetween._1.isDefined || rowsBetween._2.isDefined) {
        "rows between (" + (if(rowsBetween._1.isDefined) {rowsBetween._1.get.toSensString} else {"unbounded"}) + ", " +
          (if(rowsBetween._2.isDefined) {rowsBetween._2.get.toSensString} else {"unbounded"}) + ")\n"
      } else {""}) +
      (if(rangeBetween._1.isDefined || rangeBetween._2.isDefined) {
        "range between (" + (if(rangeBetween._1.isDefined) {rangeBetween._1.get.toSensString} else {"unbounded"}) + ", " +
          (if(rangeBetween._2.isDefined) {rangeBetween._2.get.toSensString} else {"unbounded"}) + ")\n"
      } else {""}) +
      ")"


  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    if(!WindowFunctions.numberOfArguments(functionName).contains(arguments.size)) {
      Failure(WrongFunctionArgumentsException(functionName.toString, arguments.size))
    } else if((rangeBetween._1.isDefined || rangeBetween._2.isDefined) && orderBy.isEmpty) {
      Failure(new ValidationException("Window function with RANGE requires exactly one ORDER BY column"))
    } else if((rangeBetween._1.isDefined || rangeBetween._2.isDefined) && (rowsBetween._1.isDefined || rowsBetween._2.isDefined)) {
      Failure(new ValidationException("Window function cannot have ROWS and RANGE at the same time"))
    } else {
      Try(WindowFunction(
        functionName,
        arguments.map(_.validateAndRemoveVariablePlaceholders(context).get),
        partitionBy.map(_.validateAndRemoveVariablePlaceholders(context).get),
        orderBy.map(_.validateAndRemoveVariablePlaceholders(context).get),
        (rowsBetween._1.map(_.validateAndRemoveVariablePlaceholders(context).get), rowsBetween._2.map(_.validateAndRemoveVariablePlaceholders(context).get)),
        (rangeBetween._1.map(_.validateAndRemoveVariablePlaceholders(context).get), rangeBetween._2.map(_.validateAndRemoveVariablePlaceholders(context).get))
      ))
    }
  }

  override def getSubExpressions: List[SensExpression] =
    arguments :::
      partitionBy :::
      rowsBetween._1.toList ::: rowsBetween._2.toList :::
      rangeBetween._1.toList ::: rangeBetween._2.toList

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(arguments, f)
        .orElse(collectFirstSubExpression(partitionBy, f))
        .orElse(collectFirstSubExpression(orderBy, f))
        .orElse(rowsBetween._1.flatMap(_.findSubExpression(f)))
        .orElse(rowsBetween._2.flatMap(_.findSubExpression(f)))
        .orElse(rangeBetween._1.flatMap(_.findSubExpression(f)))
        .orElse(rangeBetween._2.flatMap(_.findSubExpression(f)))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr :::
      arguments.flatMap(_.findAllSubExpressions(f)) :::
      partitionBy.flatMap(_.findAllSubExpressions(f)) :::
      orderBy.flatMap(_.findAllSubExpressions(f)) :::
      rowsBetween._1.toList.flatMap(_.findAllSubExpressions(f)) :::
      rowsBetween._2.toList.flatMap(_.findAllSubExpressions(f)) :::
      rangeBetween._1.toList.flatMap(_.findAllSubExpressions(f)) :::
      rangeBetween._2.toList.flatMap(_.findAllSubExpressions(f))
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      WindowFunction(
        functionName,
        arguments.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
        partitionBy.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
        orderBy.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
        (rowsBetween._1.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)), rowsBetween._2.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))),
        (rangeBetween._1.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)), rangeBetween._2.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)))
      )
    }
}

object WindowFunctions extends Enumeration {
  type Name = Value

  val UNKNOWN = Value(0, "?")
  val SUM = Value(1, "sum")
  val AVG = Value(2, "avg")
  val COUNT = Value(3, "count")
  val MAX = Value(4, "max")
  val MIN = Value(5, "min")

  val ROW_NUMBER = Value(6, "row_number")
  val RANK = Value(7, "rank")
  val DENSE_RANK = Value(8, "dense_rank")

  val PERCENT_RANK = Value(9, "percent_rank")
  val CUME_DIST = Value(10, "cume_dist")

  val LEAD = Value(11, "lead")
  val LAG = Value(12, "lag")
  val NTILE = Value(13, "ntile")
  val FIRST_VALUE = Value(14, "first_value")
  val LAST_VALUE = Value(15, "last_value")
  val NTH_VALUE = Value(16, "nth_value")

  def numberOfArguments(name: Name): List[Int] = {
    name match {
      case SUM => 1 :: Nil
      case AVG => 1 :: Nil
      case COUNT => 0 :: 1 :: Nil
      case MAX => 1 :: Nil
      case MIN => 1 :: Nil
      case ROW_NUMBER => 0 :: Nil
      case RANK => 0 :: Nil
      case DENSE_RANK => 0 :: Nil
      case PERCENT_RANK => 0 :: Nil
      case CUME_DIST => 0 :: Nil
      case LEAD => 1 :: 2 :: 3 :: Nil
      case LAG => 1 :: 2 :: 3 :: Nil
      case NTILE => 1 :: Nil
      case FIRST_VALUE => 1 :: Nil
      case LAST_VALUE => 1 :: Nil
      case NTH_VALUE => 2 :: Nil
      case _ => Nil
    }
  }

  def withNameUnknown(s: String): Value =
    values.find(_.toString == s).getOrElse(UNKNOWN)
}
