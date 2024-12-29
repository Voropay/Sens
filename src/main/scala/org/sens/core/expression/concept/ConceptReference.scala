package org.sens.core.expression.concept

import org.sens.core.concept.{Annotation, Attribute, ParentConcept}
import org.sens.core.expression.SensExpression
import org.sens.parser.{ElementNotFoundException, ValidationContext, WrongTypeException}

import scala.util.{Failure, Success, Try}

case class ConceptReference(conceptName: String) extends SensConceptExpression {

  override def getName: String = conceptName

  override def getAttributeNames(context: ValidationContext): List[String] = {
    val conceptDefinition = context.getConcept(conceptName)
    if (conceptDefinition.isEmpty) {
      throw ElementNotFoundException(conceptName)
    }
    conceptDefinition.get.concept.getAttributeNames(context)
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    val conceptDefinition = context.getConcept(conceptName)
    if (conceptDefinition.isEmpty) {
      throw ElementNotFoundException(conceptName)
    }
    conceptDefinition.get.concept.getAttributes(context)
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = {
    val conceptDefinition = context.getConcept(conceptName)
    if (conceptDefinition.isEmpty) {
      throw ElementNotFoundException(conceptName)
    }
    conceptDefinition.get.concept.getCurrentAttributes(context)
  }

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = {
    val conceptDefinition = context.getConcept(conceptName)
    if (conceptDefinition.isEmpty) {
      throw ElementNotFoundException(conceptName)
    }
    conceptDefinition.get.concept.getParentConcepts(context)
  }

  override def getAnnotations: List[Annotation] = List()

  override def toSensString: String = conceptName

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConceptExpression] = {
    if(!context.containsConcept(conceptName))
      Failure(ElementNotFoundException(conceptName))
    else
      Success(this)
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[SensConceptExpression] = Success(this)

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    Some(this).filter(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    if (f(this)) List(this) else Nil
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConceptExpression =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case acd: SensConceptExpression => acd
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensConceptExpression")
      }
    } else {
      this
    }
}
