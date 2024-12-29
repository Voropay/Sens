package org.sens.core.concept

import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.parser.ValidationContext

import scala.util.Try

case class CubeConcept (name: String,
                        metrics: List[Attribute],
                        dimensions: List[Attribute],
                        parentConcepts: List[ParentConcept],
                        attributeDependencies: Option[SensExpression],
                        groupDependencies: Option[SensExpression],
                        orderByAttributes: List[Order],
                        limit: Option[Int],
                        offset: Option[Int],
                        annotations: List[Annotation]
                  ) extends SensCubeConcept {
  override def getName: String = name

  override def getSubExpressions: List[SensExpression] =
    attributeDependencies.toList ::: groupDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(metrics, f)
      .orElse(collectFirstSubExpression(dimensions, f))
      .orElse(collectFirstSubExpression(parentConcepts, f))
      .orElse(attributeDependencies.flatMap(_.findSubExpression(f)))
      .orElse(groupDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(orderByAttributes, f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    metrics.flatMap(_.findAllSubExpressions(f)) :::
      dimensions.flatMap(_.findAllSubExpressions(f)) :::
      parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      attributeDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      groupDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      orderByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept = {
    CubeConcept(
      name,
      metrics.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      dimensions.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      attributeDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      groupDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      orderByAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      limit,
      offset,
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
  }

  override def getAttributeNames(context: ValidationContext): List[String] = {
    metrics.map(_.name) ::: dimensions.map(_.name)
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    metrics ::: dimensions
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = getAttributes(context)

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getAnnotations: List[Annotation] = annotations

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
      "concept cube " + name +
      "\nmetrics (" + metrics.map(_.toSensString).mkString(",\n") + ")" +
      "\ndimensions (" + dimensions.map(_.toSensString).mkString(",\n") + ")" +
      (if (parentConcepts.nonEmpty) "\nfrom " + parentConcepts.map(_.toSensString).mkString(", ") else "") +
      (if (attributeDependencies.isDefined) "\nwhere " + attributeDependencies.get.toSensString else "") +
      (if (groupDependencies.isDefined) "\nhaving " + groupDependencies.get.toSensString else "") +
      (if (orderByAttributes.nonEmpty) "\norder by " + orderByAttributes.map(_.toSensString).mkString(", ") else "") +
      (if (limit.isDefined) "\nlimit " + limit.get.toString else "") +
      (if (offset.isDefined) "\noffset " + offset.get.toString else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[CubeConcept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(CubeConcept(
      name,
      metrics.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      dimensions.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      attributeDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      orderByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      limit,
      offset,
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  override def getMetrics(context: ValidationContext): List[Attribute] = metrics

  override def getDimensions(context: ValidationContext): List[Attribute] = dimensions

  override def toCubeConcept(context: ValidationContext): CubeConcept = this

  def inferAttributeExpressions(context: ValidationContext): Try[Concept] = {
    toConcept(context).inferAttributeExpressions(context)
  }

  override def toConcept(context: ValidationContext): Concept = {
    Concept(name,
      Nil,
      metrics ::: dimensions,
      parentConcepts,
      attributeDependencies,
      dimensions.map(attr => ConceptAttribute(Nil, attr.name)),
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }
}

object CubeConcept {
  def builder(name: String,
              metrics: List[Attribute],
              dimensions: List[Attribute],
              parentConcepts: List[ParentConcept]
             ): CubeConceptBuilder = {
    new CubeConceptBuilder(name, metrics, dimensions, parentConcepts)
  }
}

class CubeConceptBuilder(name: String,
                         metrics: List[Attribute],
                         dimensions: List[Attribute],
                         parentConcepts: List[ParentConcept]) {
  var attributeDependencies: Option[SensExpression] = None
  var groupDependencies: Option[SensExpression] = None
  var orderByAttributes: List[Order] = Nil
  var limit: Option[Int] = None
  var offset: Option[Int] = None
  var annotations: List[Annotation] = Nil

  def attributeDependencies(value: SensExpression): CubeConceptBuilder = {
    attributeDependencies = Some(value)
    this
  }

  def groupDependencies(value: SensExpression): CubeConceptBuilder = {
    groupDependencies = Some(value)
    this
  }

  def orderByAttributes(value: List[Order]): CubeConceptBuilder = {
    orderByAttributes = value
    this
  }

  def limit(value: Int): CubeConceptBuilder = {
    limit = Some(value)
    this
  }

  def offset(value: Int): CubeConceptBuilder = {
    offset = Some(value)
    this
  }

  def annotations(value: List[Annotation]): CubeConceptBuilder = {
    annotations = value
    this
  }

  def build(): CubeConcept = {
    CubeConcept(
      name,
      metrics,
      dimensions,
      parentConcepts,
      attributeDependencies,
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }
}
