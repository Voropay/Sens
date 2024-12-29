package org.sens.core.expression

import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.{Failure, Try}

case class MethodInvocation(attributes: List[String], arguments: List[SensExpression]) extends SensExpression {
  override def toSensString: String = {
    attributes.mkString(".") + "(" + arguments.map(_.toSensString).mkString(", ") + ")"
  }

  def getConceptAttribute: ConceptAttribute = {
    ConceptAttribute(
      attributes.dropRight(1),
      attributes.last
    )
  }

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    val conceptAttrValidation = getConceptAttribute.validateAndRemoveVariablePlaceholders(context)
    conceptAttrValidation match {
      case Failure(err) => Failure(err)
      case _ => Try(MethodInvocation(
        attributes,
        arguments.map(_.validateAndRemoveVariablePlaceholders(context).get)
      ))
    }
  }

  override def getSubExpressions: List[SensExpression] = arguments

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(arguments, f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: arguments.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      MethodInvocation(attributes, arguments.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)))
    }
}
