package org.sens.core.concept

import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.SensLiteral
import org.sens.core.expression.GenericParameter
import org.sens.parser.{GenericDefinitionException, ValidationContext}

import scala.util.{Failure, Try}

case class ConceptAttributes(name: String,
                             genericParameters: List[String],
                             attributes: List[SensAttribute],
                             parentConcepts: List[ParentConcept],
                             annotations: List[Annotation]
                           ) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(attributes, f)
      .orElse(collectFirstSubExpression(parentConcepts, f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attributes.flatMap(_.findAllSubExpressions(f)) :::
      parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): ConceptAttributes =
    ConceptAttributes(
      name,
      genericParameters,
      attributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def getAttributeNames(context: ValidationContext): List[String] = composeAttributes(attributes, context).map(_.name)

  override def getAttributes(context: ValidationContext): List[Attribute] = composeAttributes(attributes, context)

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = composeAttributes(attributes, context)

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getGenericParameters: List[String] = genericParameters

  override def getAnnotations: List[Annotation] = annotations

  def getParentConceptAliases: List[String] = {
    parentConcepts.zipWithIndex.map(curItem =>
      curItem._1.alias.getOrElse(curItem._2.toString)
    )
  }

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
      "concept attributes " + name +
      (if (genericParameters.nonEmpty) "[" + genericParameters.mkString(", ") + "]" else "") +
      " (" + attributes.map(_.toSensString).mkString(",\n") + ")" +
      (if (parentConcepts.nonEmpty) "\nfrom " + parentConcepts.map(_.toSensString).mkString(", ") else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ConceptAttributes] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(ConceptAttributes(
      name,
      genericParameters,
      attributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  def inferAttributeExpressions(context: ValidationContext): Try[Concept] = {
    if (isGeneric) {
      return Failure(GenericDefinitionException("Cannot infer attribute expressions for generic concept definition " + name))
    }
    toConcept(context).inferAttributeExpressions(context)
  }

  def toConcept(context: ValidationContext): Concept = {
    Concept(name,
      Nil,
      getAttributes(context),
      parentConcepts,
      None,
      Nil,
      None,
      Nil,
      None,
      None,
      annotations
    )
  }

  override def getInstance(context: ValidationContext, newName: String, genericParameterValues: Map[String, SensLiteral]): ConceptAttributes = {
    if (!isGeneric) {
      this
    } else {
      var conceptWithoutGenericParameters = this
      genericParameterValues.foreach(param => {
        conceptWithoutGenericParameters = conceptWithoutGenericParameters.replaceSubExpression(GenericParameter(param._1), param._2)
      })
      val parentConceptInstances = instantiateGenericConcepts(conceptWithoutGenericParameters.parentConcepts, genericParameterValues, context)
      conceptWithoutGenericParameters.copy(name = newName, genericParameters = Nil, parentConcepts = parentConceptInstances)
    }
  }
}

object ConceptAttributes {
  def builder(name: String,
              attributes: List[SensAttribute],
              parentConcepts: List[ParentConcept]
             ): AttributeConceptBuilder = {
    new AttributeConceptBuilder(name, attributes, parentConcepts)
  }
}

class AttributeConceptBuilder(name: String,
                     attributes: List[SensAttribute],
                     parentConcepts: List[ParentConcept]) {
  var genericParameters: List[String] = Nil
  var annotations: List[Annotation] = Nil

  def genericParameters(value: List[String]): AttributeConceptBuilder = {
    genericParameters = value
    this
  }

  def annotations(value: List[Annotation]): AttributeConceptBuilder = {
    annotations = value
    this
  }

  def build(): ConceptAttributes = {
    ConceptAttributes(
      name,
      genericParameters,
      attributes,
      parentConcepts,
      annotations
    )
  }
}
