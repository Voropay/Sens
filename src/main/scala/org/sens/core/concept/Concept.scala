package org.sens.core.concept

import org.sens.core.expression.concept.{AnonymousConceptDefinition, AnonymousFunctionConceptDefinition, ConceptReference, GenericConceptReference, SensConceptExpression}
import org.sens.core.expression.literal.SensLiteral
import org.sens.core.expression.{ConceptAttribute, GenericParameter, SensExpression}
import org.sens.core.expression.operation.comparison.Equals
import org.sens.parser.{AttributeExpressionNotFound, GenericDefinitionException, ValidationContext}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class Concept(name: String,
                   genericParameters: List[String],
                   attributes: List[Attribute],
                   parentConcepts: List[ParentConcept],
                   attributeDependencies: Option[SensExpression],
                   groupByAttributes: List[SensExpression],
                   groupDependencies: Option[SensExpression],
                   orderByAttributes: List[Order],
                   limit: Option[Int],
                   offset: Option[Int],
                   annotations: List[Annotation]
                  ) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = false

  override def getSubExpressions: List[SensExpression] =
      attributeDependencies.toList ::: groupByAttributes ::: groupDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(attributes, f)
      .orElse(collectFirstSubExpression(parentConcepts, f))
      .orElse(attributeDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(groupByAttributes, f))
      .orElse(groupDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(orderByAttributes, f))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attributes.flatMap(_.findAllSubExpressions(f)) :::
      parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      attributeDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      groupByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      groupDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      orderByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): Concept =
    Concept(
      name,
      genericParameters,
      attributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      attributeDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      groupByAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      groupDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      orderByAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      limit,
      offset,
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def getAttributeNames(context: ValidationContext): List[String] = attributes.map(_.name)

  override def getAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getGenericParameters: List[String] = genericParameters

  override def getAnnotations: List[Annotation] = annotations

  def inferAttributeExpressions(context: ValidationContext): Try[Concept] = {
    if(isGeneric) {
      return Failure(GenericDefinitionException("Cannot infer attribute expressions for generic concept definition " + name))
    }
    //1. Merge attribute expressions from the child and parent concepts and dependencies into one list
    val attributeEqualityExpressions = ListBuffer[Equals]()
    val otherExpressions = ListBuffer[SensExpression]()

    //Get expressions from attribute definitions
    for(curAttr <- attributes) {
      if(curAttr.value.isDefined) {
        attributeEqualityExpressions += Equals(ConceptAttribute(Nil, curAttr.name), curAttr.value.get)
      }
    }

    getExpressionsFromParentConcepts(attributeEqualityExpressions, parentConcepts)
    getExpressionsFromDependencies(attributeEqualityExpressions, otherExpressions, attributeDependencies)

    //2. Set expressions for the attributes
    val inferredAttributesExpressions: List[Try[Attribute]] = attributes.map(curAttr => {
      if(curAttr.value.isDefined) {
        removeAttributeExpressionFromList(
          Equals(ConceptAttribute(Nil, curAttr.name), curAttr.value.get),
          attributeEqualityExpressions
        )
        Success(curAttr)
      } else {
        var expressionForAttr = pickAttributeExpressionFromList(
          ConceptAttribute(Nil, curAttr.name),
          attributeEqualityExpressions
        )
        if(expressionForAttr.isEmpty) {
          expressionForAttr = inferAttributesFromParentConceptsExpressions(curAttr.name, parentConcepts, context)
        }
        if(expressionForAttr.isEmpty) {
          Failure(AttributeExpressionNotFound(curAttr.name))
        } else {
          Success(Attribute(curAttr.name, expressionForAttr, curAttr.annotations))
        }
      }
    })

    //3. Infer expressions for the parent concept attributes
    val inferredParentConceptsExpressions = inferAttributesForParentConcepts(parentConcepts, attributeEqualityExpressions, context)

    //4. Combine rest of the attribute expressions with other dependencies
    val remainingAttributeDependencies = attributeEqualityExpressions.toList ::: otherExpressions.toList
    val inferredAttributeDependencies = joinAndExpressions(remainingAttributeDependencies)

    Try(Concept(
      name,
      genericParameters,
      inferredAttributesExpressions.map(_.get),
      inferredParentConceptsExpressions.map(_.get),
      inferredAttributeDependencies,
      groupByAttributes,
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    ))
  }

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
    "concept " + name +
    (if (genericParameters.nonEmpty) "[" + genericParameters.mkString(", ") + "]" else "") +
    " (" + attributes.map(_.toSensString).mkString(",\n") + ")" +
    (if (parentConcepts.nonEmpty) "\nfrom " + parentConcepts.map(_.toSensString).mkString(", ") else "") +
    (if (attributeDependencies.isDefined) "\nwhere " + attributeDependencies.get.toSensString else "") +
    (if (groupByAttributes.nonEmpty) "\ngroup by " + groupByAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (groupDependencies.isDefined) "\nhaving " + groupDependencies.get.toSensString else "") +
    (if (orderByAttributes.nonEmpty) "\norder by " + orderByAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (limit.isDefined) "\nlimit " + limit.get.toString else "") +
    (if (offset.isDefined) "\noffset " + offset.get.toString else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[Concept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(Concept(
      name,
      genericParameters,
      attributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      attributeDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      orderByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      limit,
      offset,
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  override def getInstance(context: ValidationContext, newName: String, genericParameterValues: Map[String, SensLiteral]): Concept = {
    if(!isGeneric) {
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

object Concept {
  def builder(name: String,
              attributes: List[Attribute],
              parentConcepts: List[ParentConcept]
             ): ConceptBuilder = {
    new ConceptBuilder(name, attributes, parentConcepts)
  }
}

class ConceptBuilder(name: String,
                     attributes: List[Attribute],
                     parentConcepts: List[ParentConcept]) {
  var genericParameters: List[String] = Nil
  var attributeDependencies: Option[SensExpression] = None
  var groupByAttributes: List[SensExpression] = Nil
  var groupDependencies: Option[SensExpression] = None
  var orderByAttributes: List[Order] = Nil
  var limit: Option[Int] = None
  var offset: Option[Int] = None
  var annotations: List[Annotation] = Nil

  def genericParameters(value: List[String]): ConceptBuilder = {
    genericParameters = value
    this
  }

  def attributeDependencies(value: SensExpression): ConceptBuilder = {
    attributeDependencies = Some(value)
    this
  }

  def groupByAttributes(value: List[SensExpression]): ConceptBuilder = {
    groupByAttributes = value
    this
  }

  def groupDependencies(value: SensExpression): ConceptBuilder = {
    groupDependencies = Some(value)
    this
  }

  def orderByAttributes(value: List[Order]): ConceptBuilder = {
    orderByAttributes = value
    this
  }

  def limit(value: Int): ConceptBuilder = {
    limit = Some(value)
    this
  }

  def offset(value: Int): ConceptBuilder = {
    offset = Some(value)
    this
  }

  def annotations(value: List[Annotation]): ConceptBuilder = {
    annotations = value
    this
  }

  def build(): Concept = {
    Concept(
      name,
      genericParameters,
      attributes,
      parentConcepts,
      attributeDependencies,
      groupByAttributes,
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }
}
