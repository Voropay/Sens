package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.core.expression.concept.SensConceptExpression
import org.sens.parser.{GenericDefinitionException, ValidationContext}

import scala.util.{Try, Failure}

case class ParentConcept(concept: SensConceptExpression,
                         alias: Option[String],
                         attributeDependencies: Map[String, SensExpression],
                         annotations: List[Annotation]) extends SensElement {
  override def toSensString: String =
    (if (annotations.nonEmpty) annotations.map(_.toSensString).mkString(", ") + " " else "") +
    concept.toSensString +
    (if (alias.isDefined) " " + alias.getOrElse("") else "") +
    (if (attributeDependencies.nonEmpty) " (" + attributeDependencies.map(el => el._1 + " = " + el._2.toSensString).mkString(", ") + ")" else "")

  def getAlias: String = alias.getOrElse(concept.getName)

  def getInstanceAlias: String = alias.getOrElse(concept.getInstanceName)

  def checkName(nameToCompare: String): Boolean = {
    if(concept.nameDefined) {
      getAlias == nameToCompare
    } else if(alias.isDefined) {
      alias.get == nameToCompare
    } else {
      false
    }
  }

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ParentConcept] = {
    Try(ParentConcept(
      concept.validateAndRemoveVariablePlaceholders(context).get,
      alias,
      attributeDependencies.map(item => item._1 -> item._2.validateAndRemoveVariablePlaceholders(context).get),
      annotations.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  def inferAttributeExpressions(context: ValidationContext): Try[ParentConcept] = {
    Try(ParentConcept(
      concept.inferAttributeExpressions(context).get,
      alias,
      attributeDependencies,
      annotations
    ))
  }

  def getSubExpressions: List[SensExpression] =
    concept :: attributeDependencies.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = {
    concept.findSubExpression(f)
      .orElse(collectFirstSubExpression(attributeDependencies.values, f))
      .orElse(collectFirstSubExpression(annotations, f))
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    concept.findAllSubExpressions(f) :::
      attributeDependencies.values.flatMap(_.findAllSubExpressions(f)).toList :::
      annotations.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): ParentConcept =
    ParentConcept(
      concept.replaceSubExpression(replaceSubExpression, withSubExpression),
      alias,
      attributeDependencies.mapValues(_.replaceSubExpression(replaceSubExpression, withSubExpression)),
      annotations.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
}
