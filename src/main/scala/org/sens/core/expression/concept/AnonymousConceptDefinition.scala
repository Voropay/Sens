package org.sens.core.expression.concept

import org.sens.core.concept.{Annotation, Attribute, Concept, Order, ParentConcept, SensConcept}
import org.sens.core.expression.literal.SensLiteral
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.parser.{AttributeExpressionNotFound, ValidationContext, WrongTypeException}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class AnonymousConceptDefinition(attributes: List[Attribute],
                                      parentConcepts: List[ParentConcept],
                                      attributeDependencies: Option[SensExpression],
                                      groupByAttributes: List[SensExpression],
                                      groupDependencies: Option[SensExpression],
                                      orderByAttributes: List[Order],
                                      limit: Option[Int],
                                      offset: Option[Int],
                                      annotations: List[Annotation]
                                     ) extends SensConceptExpression {

  val UUID = java.util.UUID.randomUUID.toString
  override def getName: String = "_anonymous_concept_" + UUID

  override def getSubExpressions: List[SensExpression] =
      attributeDependencies.toList :::
        groupByAttributes :::
        groupDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectFirstSubExpression(attributes, f)
        .orElse(collectFirstSubExpression(parentConcepts, f))
        .orElse(attributeDependencies.flatMap(_.findSubExpression(f)))
        .orElse(collectFirstSubExpression(groupByAttributes, f))
        .orElse(groupDependencies.flatMap(_.findSubExpression(f)))
        .orElse(collectFirstSubExpression(orderByAttributes, f))
        .orElse(collectFirstSubExpression(annotations, f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr :::
      attributes.flatMap(_.findAllSubExpressions(f)) :::
      parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      attributeDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      groupByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      groupDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      orderByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConceptExpression =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case acd: SensConceptExpression => acd
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensConceptExpression")
      }
    } else {
      AnonymousConceptDefinition(
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
    }

  override def getAttributeNames(context: ValidationContext): List[String] = {
    if(attributes.nonEmpty) {
      attributes.map(_.name)
    } else {
      if(parentConcepts.isEmpty) {
        throw new NotImplementedError()
      }
      parentConcepts.flatMap(_.concept.getAttributeNames(context)).distinct
    }
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    if(attributes.nonEmpty) {
      attributes
    } else {
      if(parentConcepts.isEmpty) {
        throw new NotImplementedError()
      }
      var attributesList = List[Attribute]()
      for(parentConcept <- parentConcepts) {
        val parentAttributes = parentConcept.concept.getAttributes(context)
        for(curAttr <- parentAttributes) {
          if(!attributesList.exists(item => curAttr.name == item.name)) {
            attributesList = curAttr :: attributesList
          }
        }
      }
      attributesList.reverse
    }
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = attributes

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def getAnnotations: List[Annotation] = annotations

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[AnonymousConceptDefinition] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(AnonymousConceptDefinition(
      attributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      attributeDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      orderByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      limit,
      offset,
      annotations
    ))
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[AnonymousConceptDefinition] = {
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
    val attributesNames = getAttributeNames(context)
    val inferredAttributesExpressions: List[Try[Attribute]] = attributesNames.map(curAttrName => {
      val curAttr = attributes.find(_.name == curAttrName)
      if(curAttr.isDefined && curAttr.get.value.isDefined) {
        removeAttributeExpressionFromList(
          Equals(ConceptAttribute(Nil, curAttr.get.name), curAttr.get.value.get),
          attributeEqualityExpressions
        )
        Success(curAttr.get)
      } else {
        var expressionForAttr = pickAttributeExpressionFromList(
          ConceptAttribute(Nil, curAttrName),
          attributeEqualityExpressions
        )
        if(expressionForAttr.isEmpty) {
          expressionForAttr = inferAttributesFromParentConceptsExpressions(curAttrName, parentConcepts, context)
        }
        if(expressionForAttr.isEmpty) {
          Failure(AttributeExpressionNotFound(curAttrName))
        } else {
          val annotations = if(curAttr.isDefined) curAttr.get.annotations else Nil
          Success(Attribute(curAttrName, expressionForAttr, annotations))
        }
      }
    })

    //3. Infer expressions for the parent concept attributes
    val inferredParentConceptsExpressions = inferAttributesForParentConcepts(parentConcepts, attributeEqualityExpressions, context)

    //4. Combine rest of the attribute expressions with other dependencies
    val remainingAttributeDependencies = attributeEqualityExpressions.toList ::: otherExpressions.toList
    val inferredAttributeDependencies = joinAndExpressions(remainingAttributeDependencies)

    Try(AnonymousConceptDefinition(
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

  def toConcept(): Concept = {
    Concept(
      "anonymousConceptDefinition",
      Nil,
      attributes,
      parentConcepts,
      attributeDependencies,
      groupByAttributes,
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      Nil
    )
  }

  override def getInstance(context: ValidationContext, name: String, genericParameterValues: Map[String, SensLiteral]): AnonymousConceptDefinition = {
    val parentConceptInstances = parentConcepts.map(pc => {
      val parentConceptInstance = pc.concept match {
        case gpc: GenericConceptReference =>
          val parentConceptDef = gpc.getGenericConceptInstance(context, gpc.parameters)
          ConceptReference(parentConceptDef.getName)
        case ac: AnonymousConceptDefinition =>
          ac.getInstance(context, "", genericParameterValues)
        case other => other
      }
      pc.copy(concept = parentConceptInstance)
    })
    this.copy(parentConcepts = parentConceptInstances)
  }

  override def toSensString: String =
    "< " +
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(", ") + " " else "") +
    (if (attributes.nonEmpty) "(" + attributes.map(_.toSensString).mkString(", ") + ") from " else "") +
    (if (parentConcepts.nonEmpty) parentConcepts.map(_.toSensString).mkString(", ") else "") +
    (if (attributeDependencies.isDefined) " where " + attributeDependencies.get.toSensString else "") +
    (if (groupByAttributes.nonEmpty) " group by " + groupByAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (groupDependencies.isDefined) " having " + groupDependencies.get.toSensString else "") +
    (if (orderByAttributes.nonEmpty) " order by " + orderByAttributes.map(_.toSensString).mkString(", ") else "") +
    (if (limit.isDefined) " limit " + limit.get.toString else "") +
    (if (offset.isDefined) " offset " + offset.get.toString else "") +
    " >"
}

object AnonymousConceptDefinition {
  def builder(attributes: List[Attribute],
              parentConcepts: List[ParentConcept]
             ): AnonymousConceptDefinitionBuilder = {
    new AnonymousConceptDefinitionBuilder(attributes, parentConcepts)
  }
}

class AnonymousConceptDefinitionBuilder(
                                         attributes: List[Attribute],
                                         parentConcepts: List[ParentConcept]) {
  var attributeDependencies: Option[SensExpression] = None
  var groupByAttributes: List[SensExpression] = Nil
  var groupDependencies: Option[SensExpression] = None
  var orderByAttributes: List[Order] = Nil
  var limit: Option[Int] = None
  var offset: Option[Int] = None
  var annotations: List[Annotation] = Nil

  def attributeDependencies(value: SensExpression): AnonymousConceptDefinitionBuilder = {
    attributeDependencies = Some(value)
    this
  }

  def groupByAttributes(value: List[SensExpression]): AnonymousConceptDefinitionBuilder = {
    groupByAttributes = value
    this
  }

  def groupDependencies(value: SensExpression): AnonymousConceptDefinitionBuilder = {
    groupDependencies = Some(value)
    this
  }

  def orderByAttributes(value: List[Order]): AnonymousConceptDefinitionBuilder = {
    orderByAttributes = value
    this
  }

  def limit(value: Int): AnonymousConceptDefinitionBuilder = {
    limit = Some(value)
    this
  }

  def offset(value: Int): AnonymousConceptDefinitionBuilder = {
    offset = Some(value)
    this
  }

  def annotations(value: List[Annotation]): AnonymousConceptDefinitionBuilder = {
    annotations = value
    this
  }

  def build(): AnonymousConceptDefinition = {
    AnonymousConceptDefinition(
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
