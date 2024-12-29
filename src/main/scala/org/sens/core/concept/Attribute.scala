package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class Attribute(name: String,
                     value: Option[SensExpression],
                     annotations: List[Annotation]) extends SensElement {
  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(", ") + " " else "") +
    name +
    (if (value.isDefined) " = " + value.get.toSensString else "")

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[Attribute] = {
    Try(Attribute(
      name,
      value.map(_.validateAndRemoveVariablePlaceholders(context).get),
      annotations.map(_.validateAndRemoveVariablePlaceholders(context).get)
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
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
}
