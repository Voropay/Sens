package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.expression.operation.logical.{AndSeq, And}
import org.sens.parser.{ElementNotFoundException, ValidationContext, WrongTypeException}

import scala.util.Try

case class CubeInheritedConcept(name: String,
                                parentConcept: ParentConcept,
                                overriddenMetrics: List[Attribute],
                                removedMetrics: List[ConceptAttribute],
                                overriddenDimensions: List[Attribute],
                                removedDimensions: List[ConceptAttribute],
                                additionalDependencies: Option[SensExpression],
                                groupDependencies: Option[SensExpression],
                                orderByAttributes: List[Order],
                                limit: Option[Int],
                                offset: Option[Int],
                                annotations: List[Annotation]
                               ) extends SensCubeConcept {

  override def getName: String = name

  override def getSubExpressions: List[SensExpression] =
    additionalDependencies.toList ::: groupDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    parentConcept.findSubExpression(f)
      .orElse(collectFirstSubExpression(overriddenMetrics, f))
      .orElse(collectFirstSubExpression(removedMetrics, f))
      .orElse(collectFirstSubExpression(overriddenDimensions, f))
      .orElse(collectFirstSubExpression(removedDimensions, f))
      .orElse(additionalDependencies.flatMap(_.findSubExpression(f)))
      .orElse(groupDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(orderByAttributes, f))
      .orElse(collectFirstSubExpression(annotations, f))
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    parentConcept.findAllSubExpressions(f) :::
      overriddenMetrics.flatMap(_.findAllSubExpressions(f)) :::
      removedMetrics.flatMap(_.findAllSubExpressions(f)) :::
      overriddenDimensions.flatMap(_.findAllSubExpressions(f)) :::
      removedDimensions.flatMap(_.findAllSubExpressions(f)) :::
      additionalDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      groupDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      orderByAttributes.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept = {
    val newRemovedMetrics = removedMetrics.map(attr => {
      attr.replaceSubExpression(replaceSubExpression, withSubExpression) match {
        case newAttr: ConceptAttribute => newAttr
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "ConceptAttribute")
      }
    })
    val newRemovedDimensions = removedDimensions.map(attr => {
      attr.replaceSubExpression(replaceSubExpression, withSubExpression) match {
        case newAttr: ConceptAttribute => newAttr
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "ConceptAttribute")
      }
    })
    CubeInheritedConcept(
      name,
      parentConcept.replaceSubExpression(replaceSubExpression, withSubExpression),
      overriddenMetrics.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      newRemovedMetrics,
      overriddenDimensions.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      newRemovedDimensions,
      additionalDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      groupDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      orderByAttributes.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      limit,
      offset,
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
  }

  def getParentCubeConceptDefinition(context: ValidationContext): SensCubeConcept = {
    parentConcept.concept match {
      case ref: ConceptReference =>
        val conDef = context.getConcept(ref.conceptName)
        if(conDef.isEmpty) {
          throw ElementNotFoundException(ref.conceptName)
        }
        conDef.get.concept match {
          case cube: SensCubeConcept => cube
          case other => throw WrongTypeException(name, CubeConcept.getClass.getTypeName, other.getClass.getTypeName)
        }
      case other => throw WrongTypeException(name, CubeConcept.getClass.getTypeName, other.getClass.getTypeName)
    }
  }

  def isAttributeRemoved(alias: String, name: String, removedAttributes: List[ConceptAttribute]): Boolean = {
    removedAttributes.exists(attr =>
      attr.attribute == name && (attr.conceptsChain.isEmpty || (attr.conceptsChain.size == 1 && attr.conceptsChain.head == alias))
    )
  }

  def isAttributeOverridden(name: String, overriddenAttributes: List[Attribute]): Boolean = {
    overriddenAttributes.exists(attr =>
      attr.name == name)
  }

  def mergeAttributes(
                       parentAttributes: List[Attribute],
                       overriddenAttributes: List[Attribute],
                       removedAttributes: List[ConceptAttribute],
                       alias: String
                     ): List[Attribute] = {
    var attributes = List[Attribute]()
    for (curAttr <- parentAttributes) {
      if (!attributes.exists(item => curAttr.name == item.name) &&
        !isAttributeRemoved(alias, curAttr.name, removedAttributes) &&
        !isAttributeOverridden(curAttr.name, overriddenAttributes)) {
        attributes = Attribute(curAttr.name, Some(ConceptAttribute(alias :: Nil, curAttr.name)), curAttr.annotations) :: attributes
      }
    }
    attributes = attributes.reverse ::: overriddenAttributes
    attributes = attributes.distinct
    attributes
  }

  override def getMetrics(context: ValidationContext): List[Attribute] = {
    val parentCubeDef = getParentCubeConceptDefinition(context)
    val parentMetrics = parentCubeDef.getMetrics(context)
    val alias = parentConcept.getAlias
    mergeAttributes(parentMetrics, overriddenMetrics, removedMetrics, alias)
  }

  override def getDimensions(context: ValidationContext): List[Attribute] = {
    val parentCubeDef = getParentCubeConceptDefinition(context)
    val parentDimensions = parentCubeDef.getDimensions(context)
    val alias = parentConcept.getAlias
    mergeAttributes(parentDimensions, overriddenDimensions, removedDimensions, alias)
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    getMetrics(context) ::: getDimensions(context)
  }

  override def getAttributeNames(context: ValidationContext): List[String] = {
    getAttributes(context).map(_.name)
  }

  override def getAnnotations: List[Annotation] = annotations

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = getAttributes(context) //overriddenMetrics ::: overriddenDimensions

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcept :: Nil

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(",\n") + "\n" else "") +
      "concept cube " + name +
      " is " + parentConcept.toSensString +
      (if (overriddenMetrics.nonEmpty) "\nwith metrics " + overriddenMetrics.map(_.toSensString).mkString(", ") else "") +
      (if (removedMetrics.nonEmpty) "\nwithout metrics " + removedMetrics.map(_.toSensString).mkString(", ") else "") +
      (if (overriddenDimensions.nonEmpty) "\nwith dimensions " + overriddenDimensions.map(_.toSensString).mkString(", ") else "") +
      (if (removedDimensions.nonEmpty) "\nwithout dimensions " + removedDimensions.map(_.toSensString).mkString(", ") else "") +
      (if (additionalDependencies.isDefined) "\nwhere " + additionalDependencies.get.toSensString else "") +
      (if (groupDependencies.isDefined) "\nhaving " + groupDependencies.get.toSensString else "") +
      (if (orderByAttributes.nonEmpty) "\norder by " + orderByAttributes.map(_.toSensString).mkString(", ") else "") +
      (if (limit.isDefined) "\nlimit " + limit.get.toString else "") +
      (if (offset.isDefined) "\noffset " + offset.get.toString else "")


  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[CubeInheritedConcept] = {
    val newContext = context.addFrame
    newContext.setCurrentConcept(this)

    Try(CubeInheritedConcept(
      name,
      parentConcept.validateAndRemoveVariablePlaceholders(newContext).get,
      overriddenMetrics.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      removedMetrics.map((_.validateAndRemoveVariablePlaceholders(newContext).get)).map(_.asInstanceOf[ConceptAttribute]),
      overriddenDimensions.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      removedDimensions.map((_.validateAndRemoveVariablePlaceholders(newContext).get)).map(_.asInstanceOf[ConceptAttribute]),
      additionalDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      groupDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      orderByAttributes.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      limit,
      offset,
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  def inferAttributeExpressions(context: ValidationContext): Try[Concept] = {
    toConcept(context).inferAttributeExpressions(context)
  }

  def removeParentConceptAlias(element: SensElement, alias: String): SensElement = {
    val attributesToReplace = element.findAllSubExpressions({
      case ConceptAttribute(parentConcepts, _) if parentConcepts.nonEmpty && parentConcepts.head == alias => true
      case _ => false
    }).map(_.asInstanceOf[ConceptAttribute])
    var updatedElement = element
    attributesToReplace.foreach(curAttr => {
      val newAttr = ConceptAttribute(curAttr.conceptsChain.tail, curAttr.attribute)
      updatedElement = updatedElement.replaceSubExpression(curAttr, newAttr)
    })
    updatedElement
  }

  def extractParentConceptDependencies(parentConcept: ParentConcept): Option[SensExpression] = {
    if(parentConcept.attributeDependencies.isEmpty) {
      None
    } else {
      val attributeDependencies = parentConcept.attributeDependencies.map(dep =>
        Equals(ConceptAttribute(parentConcept.getAlias :: Nil, dep._1), dep._2)
      )
      Some(AndSeq(attributeDependencies.toList))
    }
  }

  def replaceParentMetrics(attribute: Attribute, parentMetrics: List[Attribute], conceptChain: List[String]): Attribute = {
    var updatedAttribute = attribute
    removedMetrics.foreach(curRemovedAttr => {
      val attrToCheck = if(curRemovedAttr.conceptsChain == conceptChain) {
        curRemovedAttr
      } else {
        ConceptAttribute(conceptChain, curRemovedAttr.attribute)
      }
      if (updatedAttribute.containsSubExpression(attrToCheck)) {
        val curRemovedAttrValue = parentMetrics.find(_.name == attrToCheck.attribute)
        if (curRemovedAttrValue.isDefined && curRemovedAttrValue.get.value.isDefined) {
          updatedAttribute = updatedAttribute.replaceSubExpression(attrToCheck, curRemovedAttrValue.get.value.get)
          updatedAttribute = replaceParentMetrics(updatedAttribute, parentMetrics, conceptChain)
        }
      }
    })
    updatedAttribute
  }

  def toCubeConcept(context: ValidationContext): CubeConcept = {
    val parentConceptDef = getParentCubeConceptDefinition(context).toCubeConcept(context)
    val alias = parentConcept.getAlias

    val parentMetrics = parentConceptDef.getMetrics(context)
    val currentMetrics = overriddenMetrics
      .map(replaceParentMetrics(_, parentMetrics, alias :: Nil))
      .map(removeParentConceptAlias(_, alias))
      .map(_.asInstanceOf[Attribute])

    val parentMetricsFiltered = parentMetrics.filter(curMetric =>
      !isAttributeRemoved(alias, curMetric.name, removedMetrics) && !isAttributeOverridden(curMetric.name, currentMetrics)
    ).map(replaceParentMetrics(_, parentMetrics, Nil))

    val newMetrics = parentMetricsFiltered ::: currentMetrics

    val currentDimensions = overriddenDimensions.map(removeParentConceptAlias(_, alias)).map(_.asInstanceOf[Attribute])
    val parentDimensions = parentConceptDef.getDimensions(context).filter(curDimension =>
      !isAttributeRemoved(alias, curDimension.name, removedDimensions) && !isAttributeOverridden(curDimension.name, currentDimensions)
    )
    val newDimensions = parentDimensions ::: currentDimensions

    val extractedParentConceptDependencies = extractParentConceptDependencies(parentConcept)
      .map(removeParentConceptAlias(_, alias))
      .map(_.asInstanceOf[SensExpression])
    val currentDependecies = additionalDependencies
      .map(removeParentConceptAlias(_, alias))
      .map(_.asInstanceOf[SensExpression])
    val combinedDependencies = parentConceptDef.attributeDependencies.toList ::: currentDependecies.toList ::: extractedParentConceptDependencies.toList
    val newAttributeDependencies = if(combinedDependencies.isEmpty) {
      None
    } else if(combinedDependencies.size == 1) {
      Some(combinedDependencies.head)
    } else {
      Some(AndSeq(combinedDependencies))
    }

    val newGroupDependencies = groupDependencies.map(removeParentConceptAlias(_, alias).asInstanceOf[SensExpression])
    val newOrder = orderByAttributes.map(removeParentConceptAlias(_, alias).asInstanceOf[Order])
    val newAnnotations = annotations.map(removeParentConceptAlias(_, alias).asInstanceOf[Annotation])

    CubeConcept(
      name,
      newMetrics,
      newDimensions,
      parentConceptDef.parentConcepts,
      newAttributeDependencies,
      newGroupDependencies,
      newOrder,
      limit,
      offset,
      newAnnotations
    )
  }

  def toConcept(context: ValidationContext): Concept = {
    toCubeConcept(context).toConcept(context)
  }
}

object CubeInheritedConcept {
  def builder(name: String,
              parentConcept: ParentConcept
             ): CubeInheritedConceptBuilder = {
    new CubeInheritedConceptBuilder(name, parentConcept)
  }
}

class CubeInheritedConceptBuilder(name: String,
                         parentConcept: ParentConcept) {
  var overriddenMetrics: List[Attribute] = Nil
  var removedMetrics: List[ConceptAttribute] = Nil
  var overriddenDimensions: List[Attribute] = Nil
  var removedDimensions: List[ConceptAttribute] = Nil
  var attributeDependencies: Option[SensExpression] = None
  var groupDependencies: Option[SensExpression] = None
  var orderByAttributes: List[Order] = Nil
  var limit: Option[Int] = None
  var offset: Option[Int] = None
  var annotations: List[Annotation] = Nil

  def overriddenMetrics(value: List[Attribute]): CubeInheritedConceptBuilder = {
    overriddenMetrics = value
    this
  }

  def removedMetrics(value: List[ConceptAttribute]): CubeInheritedConceptBuilder = {
    removedMetrics = value
    this
  }

  def overriddenDimensions(value: List[Attribute]): CubeInheritedConceptBuilder = {
    overriddenDimensions = value
    this
  }

  def removedDimensions(value: List[ConceptAttribute]): CubeInheritedConceptBuilder = {
    removedDimensions = value
    this
  }

  def attributeDependencies(value: SensExpression): CubeInheritedConceptBuilder = {
    attributeDependencies = Some(value)
    this
  }

  def groupDependencies(value: SensExpression): CubeInheritedConceptBuilder = {
    groupDependencies = Some(value)
    this
  }

  def orderByAttributes(value: List[Order]): CubeInheritedConceptBuilder = {
    orderByAttributes = value
    this
  }

  def limit(value: Int): CubeInheritedConceptBuilder = {
    limit = Some(value)
    this
  }

  def offset(value: Int): CubeInheritedConceptBuilder = {
    offset = Some(value)
    this
  }

  def annotations(value: List[Annotation]): CubeInheritedConceptBuilder = {
    annotations = value
    this
  }

  def build(): CubeInheritedConcept = {
    CubeInheritedConcept(
      name,
      parentConcept,
      overriddenMetrics,
      removedMetrics,
      overriddenDimensions,
      removedDimensions,
      attributeDependencies,
      groupDependencies,
      orderByAttributes,
      limit,
      offset,
      annotations
    )
  }
}
