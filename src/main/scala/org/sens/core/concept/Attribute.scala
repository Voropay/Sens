package org.sens.core.concept

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.SensTypeLiteral
import org.sens.parser.ValidationContext

import scala.util.Try

case class Attribute(name: String,
                     value: Option[SensExpression],
                     annotations: List[Annotation],
                     sensType: Option[SensTypeLiteral] = None) extends SensAttribute {
  override def getAttributeNames(context: ValidationContext): List[String] = name :: Nil

  override def getAttributes(context: ValidationContext): List[Attribute] = this :: Nil
  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(", ") + " " else "") +
    (if (sensType.isDefined) sensType.get.toSensString else "") +
    name +
    (if (value.isDefined) " = " + value.get.toSensString else "")

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[Attribute] = {
    Try(Attribute(
      name,
      value.map(_.validateAndRemoveVariablePlaceholders(context).get),
      annotations.map(_.validateAndRemoveVariablePlaceholders(context).get),
      sensType
    ))
  }

  override def getSubExpressions: List[SensExpression] = value.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    value.flatMap(_.findSubExpression(f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    value.toList.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): Attribute =
    Attribute(
      name,
      value.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      sensType.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
}
