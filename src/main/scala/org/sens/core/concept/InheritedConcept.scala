package org.sens.core.concept

import org.sens.core.expression.literal.SensLiteral
import org.sens.core.expression.{ConceptAttribute, GenericParameter, SensExpression}
import org.sens.parser.{GenericDefinitionException, ValidationContext, WrongTypeException}

import scala.util.{Failure, Try}

case class InheritedConcept(name: String,
                            genericParameters: List[String],
                            parentConcepts: List[ParentConcept],
                            overriddenAttributes: List[Attribute],
                            removedAttributes: List[ConceptAttribute],
                            additionalDependencies: Option[SensExpression],
                            orderByAttributes: List[Order],
                            limit: Option[Int],
                            offset: Option[Int],
                            annotations: List[Annotation]
                           ) extends SensConcept {

  override def getName: String = name

  override def getGenericParameters: List[String] = genericParameters

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] =
    additionalDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(parentConcepts, f)
      .orElse(collectFirstSubExpression(overriddenAttributes, f))
      .orElse(collectFirstSubExpression(removedAttributes, f))
      .orElse(additionalDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(orderByAttributes, f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      overriddenAttributes.flatMap(_.findAllSubExpressions(f)) :::
      removedAttributes.flatMap(_.findAllSubExpressions(f)) :::
      additionalDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      orderByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): InheritedConcept = {
    val newRemovedAttributes = removedAttributes.map(attr => {
      attr.replaceSubExpression(replaceSubExpression, withSubExpression) match {
        case newAttr: ConceptAttribute => newAttr
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "ConceptAttribute")
      }
    })
    InheritedConcept(
      name,
      genericParameters,
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      overriddenAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      newRemovedAttributes,
      additionalDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      orderByAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      limit,
      offset,
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
  }

  override def getAttributeNames(context: ValidationContext): List[String] = {
    var attributeNames = List[String]()
    for(parentConcept <- parentConcepts) {
      val attributes = parentConcept.concept.getAttributeNames(context)
      val alias = parentConcept.getAlias
      for(curAttr <- attributes) {
        if(!attributeNames.contains(curAttr) && !attributeIsRemoved(alias, curAttr) && !attributeIsOverridden(curAttr)) {
          attributeNames = curAttr :: attributeNames
        }
      }
    }
    attributeNames = attributeNames.reverse ::: overriddenAttributes.map(_.name)
    attributeNames = attributeNames.distinct
    attributeNames
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    var attributesList = List[Attribute]()
    for(parentConcept <- parentConcepts) {
      val attributes = parentConcept.concept.getAttributes(context)
      val alias = parentConcept.getAlias
      for(curAttr <- attributes) {
        if(!attributesList.exists(item => curAttr.name == item.name) && !attributeIsRemoved(alias, curAttr.name) && !attributeIsOverridden(curAttr.name)) {
          attributesList = Attribute(curAttr.name, Some(ConceptAttribute(alias :: Nil, curAttr.name)), curAttr.annotations) :: attributesList
        }
      }
    }
    attributesList = attributesList.reverse ::: overriddenAttributes
    attributesList = attributesList.distinct
    attributesList
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = overriddenAttributes

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getAnnotations: List[Annotation] = annotations

  def attributeIsRemoved(alias: String, name: String): Boolean = {
    removedAttributes.exists(attr =>
      attr.attribute == name && (attr.conceptsChain.isEmpty || (attr.conceptsChain.size == 1 && attr.conceptsChain.head == alias))
    )
  }

  def attributeIsOverridden(name: String): Boolean = {
    overriddenAttributes.exists(attr =>
      attr.name == name)
  }

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString("\n") + "\n" else "") +
    "concept " + name +
    (if (genericParameters.nonEmpty) "[" + genericParameters.mkString(", ") + "]" else "") +
    " is " + parentConcepts.map(_.toSensString).mkString(", ") +
    (if (overriddenAttributes.nonEmpty) "\nwith " + overriddenAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (removedAttributes.nonEmpty) "\nwithout " + removedAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (additionalDependencies.isDefined) "\nwhere " + additionalDependencies.get.toSensString else "") +
    (if (orderByAttributes.nonEmpty) "\norder by " + orderByAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (limit.isDefined) "\nlimit " + limit.get.toString else "") +
    (if (offset.isDefined) "\noffset " + offset.get.toString else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[InheritedConcept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(InheritedConcept(
      name,
      genericParameters,
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      overriddenAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      removedAttributes.map((_.validateAndRemoveVariablePlaceholders(newContext).get)).map(_.asInstanceOf[ConceptAttribute]),
      additionalDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      orderByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      limit,
      offset,
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
      additionalDependencies,
      Nil,
      None,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }

  override def getInstance(context: ValidationContext, newName: String, genericParameterValues: Map[String, SensLiteral]): InheritedConcept = {
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

object InheritedConcept {
  def builder(name: String,
              parentConcepts: List[ParentConcept]
             ): InheritedConceptBuilder = {
    new InheritedConceptBuilder(name, parentConcepts)
  }
}

class InheritedConceptBuilder(name: String,
                     parentConcepts: List[ParentConcept]) {
  var genericParameters: List[String] = Nil
  var overriddenAttributes: List[Attribute] = Nil
  var removedAttributes: List[ConceptAttribute] = Nil
  var additionalDependencies: Option[SensExpression] = None
  var orderByAttributes: List[Order] = Nil
  var limit: Option[Int] = None
  var offset: Option[Int] = None
  var annotations: List[Annotation] = Nil

  def genericParameters(value: List[String]): InheritedConceptBuilder = {
    genericParameters = value
    this
  }

  def additionalDependencies(value: SensExpression): InheritedConceptBuilder = {
    additionalDependencies = Some(value)
    this
  }

  def overriddenAttributes(value: List[Attribute]): InheritedConceptBuilder = {
    overriddenAttributes = value
    this
  }

  def removedAttributes(value: List[ConceptAttribute]): InheritedConceptBuilder = {
    removedAttributes = value
    this
  }

  def orderByAttributes(value: List[Order]): InheritedConceptBuilder = {
    orderByAttributes = value
    this
  }

  def limit(value: Int): InheritedConceptBuilder = {
    limit = Some(value)
    this
  }

  def offset(value: Int): InheritedConceptBuilder = {
    offset = Some(value)
    this
  }

  def annotations(value: List[Annotation]): InheritedConceptBuilder = {
    annotations = value
    this
  }

  def build(): InheritedConcept = {
    InheritedConcept(
      name,
      genericParameters,
      parentConcepts,
      overriddenAttributes,
      removedAttributes,
      additionalDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }
}
