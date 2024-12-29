package org.sens.core.concept

import org.sens.core.datasource.DataSource
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.parser.ValidationContext

import scala.util.{Success, Try}

case class DataSourceConcept(name: String,
                             attributes: List[Attribute],
                             source: DataSource,
                             annotations: List[Annotation]) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(attributes, f)
      .orElse(source.findSubExpression(f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attributes.flatMap(_.findAllSubExpressions(f)) :::
      source.findAllSubExpressions(f) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept =
    DataSourceConcept(
      name,
      attributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      source.replaceSubExpression(replaceSubExpression, withSubExpression),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def getAttributeNames(context: ValidationContext): List[String] = attributes.map(_.name)

  override def getAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = Nil

  override def getAnnotations: List[Annotation] = annotations

  override def toSensString: String = {
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
    "datasource " + name +
    " (" + attributes.map(_.toSensString).mkString(",\n") + ")\n" +
    "from " + source.toSensString
  }

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConcept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(DataSourceConcept(
      name,
      attributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      source,
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[SensConcept] = Success(this)

  def toConcept(): Concept = {
    Concept.builder(
      "_datasource_" + name,
      attributes.map(attr => Attribute(attr.name, Some(ConceptAttribute("t" :: Nil, attr.name)), attr.annotations)),
      ParentConcept(ConceptReference(name), Some("t"), Map(), Nil) :: Nil
    ).build()
  }
}
