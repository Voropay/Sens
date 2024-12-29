package org.sens.core.concept

import org.sens.core.expression.SensExpression
import org.sens.parser.{AttributeNamesDoNotMatch, ValidationContext}

import scala.util.{Failure, Try}

case class IntersectConcept(name: String,
                            parentConcepts: List[ParentConcept],
                            annotations: List[Annotation]
                           ) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(parentConcepts, f)
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept =
    IntersectConcept(
      name,
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def getAttributeNames(context: ValidationContext): List[String] = {
    parentConcepts.head.concept.getAttributeNames(context)
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    val attributesNames = parentConcepts.head.concept.getAttributeNames(context)
    attributesNames.map(curAttrName => {
      val isEmpty = parentConcepts.exists(pc => {
        val curAttributes = pc.concept.getAttributes(context)
        curAttributes.exists(curAttr => curAttr.name == curAttrName && curAttr.annotations.contains(Annotation.OPTIONAL))
      })
      Attribute(curAttrName, None, if(isEmpty) Annotation.OPTIONAL :: Nil else Nil)
    })
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = getAttributes(context)

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getAnnotations: List[Annotation] = annotations

  override def toSensString: String =
    (if (!annotations.isEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
      "concept " + name + " intersect of\n" + parentConcepts.map(_.toSensString).mkString(",\n");

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[IntersectConcept] = {
    val newContext = context.addFrame
    val attributes = Try(getAttributes(context))
    if(attributes.isFailure) {
      return Failure(attributes.failed.get)
    }
    newContext.setCurrentConcept(this)

    val attributeNames = Try(parentConcepts.map(_.concept.getAttributeNames(newContext)))
    if(attributeNames.isFailure) {
      return Failure(attributeNames.failed.get)
    }
    if(!equalAttributeNames(attributeNames.get)) {
      Failure(AttributeNamesDoNotMatch())
    } else {
      Try(IntersectConcept(
        name,
        parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
        annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
      ))
    }
  }

  def inferAttributeExpressions(context: ValidationContext): Try[IntersectConcept] = {
    Try(IntersectConcept(
      name,
      parentConcepts.map(_.inferAttributeExpressions(context)).map(_.get),
      annotations
    ))
  }
}