package org.sens.core.concept

import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.expression.{ConceptObject, SensExpression}
import org.sens.parser.ValidationContext

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

case class AggregationConcept(name: String,
                              parentConcepts: List[ParentConcept],
                              attributeDependencies: Option[SensExpression],
                              annotations: List[Annotation]) extends SensConcept {

  override def getName: String = name

  override def isTransparent: Boolean = true

  override def getSubExpressions: List[SensExpression] =
    attributeDependencies.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(parentConcepts, f)
      .orElse(attributeDependencies.flatMap(_.findSubExpression(f)))
      .orElse(collectFirstSubExpression(annotations, f))

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    parentConcepts.flatMap(_.findAllSubExpressions(f)) :::
      attributeDependencies.toList.flatMap(_.findAllSubExpressions(f)) :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept =
    AggregationConcept(
      name,
      parentConcepts.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      attributeDependencies.map(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def getAttributeNames(context: ValidationContext): List[String] = {
    parentConcepts.map(curConcept =>
      curConcept.getAlias
    )
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    parentConcepts.map(curConcept => {
      val annotations: List[Annotation] = if (curConcept.annotations.contains(Annotation.OPTIONAL)) {
        Annotation.OPTIONAL :: Nil
      } else {
        Nil
      }
      Attribute(curConcept.getAlias, Some(ConceptObject(curConcept.getAlias)), annotations)
    })
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = {
    parentConcepts.map(curConcept => {
      Attribute(curConcept.getAlias, Some(ConceptObject(curConcept.getAlias)), Nil)
    })
  }

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = parentConcepts

  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString("\n") + "\n" else "") +
    "concept " + name +
    " between " + parentConcepts.map(_.toSensString).mkString(", ") +
    (if (attributeDependencies.isDefined) "\nwhere " + attributeDependencies.get.toSensString else "")

  override def getAnnotations: List[Annotation] = annotations

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[AggregationConcept] = {
    val newContext = context.addFrame

    newContext.setCurrentConcept(this)

    Try(AggregationConcept(
      name,
      parentConcepts.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      attributeDependencies.map(_.validateAndRemoveVariablePlaceholders(newContext).get),
      annotations.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  def inferAttributeExpressions(context: ValidationContext): Try[Concept] = {
    //1. Merge attribute expressions from the child and parent concepts and dependencies into one list
    val attributeEqualityExpressions = ListBuffer[Equals]()
    val otherExpressions = ListBuffer[SensExpression]()

    getExpressionsFromParentConcepts(attributeEqualityExpressions, parentConcepts)
    getExpressionsFromDependencies(attributeEqualityExpressions, otherExpressions, attributeDependencies)

    //2. Set expressions for the attributes
    val inferredAttributesExpressions: List[Attribute] = parentConcepts.map(
      curConcept => Attribute(curConcept.getAlias, Some(ConceptObject(curConcept.getAlias)), Nil)
    )

    //3. Infer expressions for the parent concept attributes
    val inferredParentConceptsExpressions = inferAttributesForParentConcepts(parentConcepts, attributeEqualityExpressions, context)

    //4. Combine rest of the attribute expressions with other dependencies
    val remainingAttributeDependencies = attributeEqualityExpressions.toList ::: otherExpressions.toList
    val inferredAttributeDependencies = joinAndExpressions(remainingAttributeDependencies)

    Try(Concept(
      name,
      Nil,
      inferredAttributesExpressions,
      inferredParentConceptsExpressions.map(_.get),
      inferredAttributeDependencies,
      Nil,
      None,
      Nil,
      None,
      None,
      annotations
    ))
  }

  def toConcept(context: ValidationContext): Concept = {
    Concept(name,
      Nil,
      getAttributes(context),
      parentConcepts,
      attributeDependencies,
      Nil,
      None,
      Nil,
      None,
      None,
      annotations
    )
  }
}

object AggregationConcept {
  def builder(name: String,
              parentConcepts: List[ParentConcept]
             ): AggregationConceptBuilder = {
    new AggregationConceptBuilder(name, parentConcepts)
  }
}

class AggregationConceptBuilder(name: String,
                     parentConcepts: List[ParentConcept]) {
  var attributeDependencies: Option[SensExpression] = None
  var annotations: List[Annotation] = Nil

  def attributeDependencies(value: SensExpression): AggregationConceptBuilder = {
    attributeDependencies = Some(value)
    this
  }

  def annotations(value: List[Annotation]): AggregationConceptBuilder = {
    annotations = value
    this
  }

  def build(): AggregationConcept = {
    AggregationConcept(
      name,
      parentConcepts,
      attributeDependencies,
      annotations
    )
  }
}