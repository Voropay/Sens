package org.sens.core.concept

import org.sens.core.expression.SensExpression
import org.sens.core.expression.function.SensFunction
import org.sens.parser.{ValidationContext, WrongFunctionArgumentsException, WrongTypeException}

import scala.util.{Failure, Success, Try}

case class FunctionConcept(name: String,
                           attributes: List[Attribute],
                           parentConcepts: List[ParentConcept],
                           function: SensFunction,
                           annotations: List[Annotation]
                          ) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] =
    function :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(attributes, f)
      .orElse(collectFirstSubExpression(parentConcepts, f))
      .orElse(function.findSubExpression(f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attributes.flatMap(_.findAllSubExpressions(f)) :::
      parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      function.findAllSubExpressions(f) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept = {
    val newFunction = function.replaceSubExpression(replaceSubExpression, withSubExpression) match {
      case func: SensFunction => func
      case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensFunction")
    }

    FunctionConcept(
      name,
      attributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      newFunction,
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
  }

  override def getAttributeNames(context: ValidationContext): List[String] =
    if(!attributes.isEmpty) {
      attributes.map(_.name)
    } else {
      throw new NotImplementedError()
    }

  override def getAttributes(context: ValidationContext): List[Attribute] =
    if(attributes.nonEmpty) {
      attributes
    } else {
      throw new NotImplementedError()
    }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getAnnotations: List[Annotation] = annotations

  def getInputAttributes: List[Attribute] = attributes.filter(_.annotations.contains(Annotation.INPUT))

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString("\n") + "\n" else "") +
    "concept " + name +
    (if (attributes.nonEmpty) " (" + attributes.map(_.toSensString).mkString(", ") + ")" else "") +
    (if (parentConcepts.nonEmpty) "\nfrom " + parentConcepts.map(_.toSensString).mkString(", ") else "") +
    "\nby " + function.toSensString

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[FunctionConcept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(FunctionConcept(
      name,
      attributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      function.validateAndRemoveVariablePlaceholders(newContext).get,
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[SensConcept] = {
    Success(this)
  }
}
