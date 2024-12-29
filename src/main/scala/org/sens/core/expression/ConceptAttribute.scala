package org.sens.core.expression

import org.sens.core.concept.{AggregationConcept, CubeConcept, CubeInheritedConcept, SensConcept, SensCubeConcept}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.{Failure, Success, Try}

case class ConceptAttribute(conceptsChain: List[String], attribute: String) extends SensExpression {
  override def toSensString: String =
    (if (conceptsChain != Nil || conceptsChain.nonEmpty) conceptsChain.mkString(".") + "." else "") + attribute

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    validateAndRemoveVariablePlaceholdersForConceptsChain(context, conceptsChain)
  }

  def validateAndRemoveVariablePlaceholdersForConceptsChain(context: ValidationContext, conceptsChain: List[String]): Try[SensExpression] = {
    if (conceptsChain.isEmpty) {
      if (!context.containsAttribute(attribute) && !context.containsParentConceptAttribute(attribute))
        return Failure(ElementNotFoundException(toSensString))
      else
        return Success(this)
    }
    if (!context.containsParentConcept(conceptsChain.head)) {
      return Failure(ElementNotFoundException(conceptsChain.head))
    }
    val parentConceptDef = context.getParentConcept(conceptsChain.head)
    val parentConceptAttributes = Try(parentConceptDef.get.concept.getAttributeNames(context))
    if (parentConceptAttributes.isFailure) {
      return Failure(parentConceptAttributes.failed.get)
    }

    if(conceptsChain.size == 1) {
      if (parentConceptDef.get.concept.isGeneric || parentConceptAttributes.get.contains(attribute)) {
        return Success(this)
      } else {
        return Failure(ElementNotFoundException(attribute))
      }
    }

    val conceptDef = parentConceptDef.get.concept match {
      case ConceptReference(name) =>
        val conDef = context.getConcept(name)
        if (conDef.isEmpty) {
          return Failure(ElementNotFoundException(name))
        }
        conDef.get.concept
      case anonConDef: AnonymousConceptDefinition => anonConDef
      case _ => throw new NotImplementedError()
    }
    if(!conceptDef.isTransparent) {
      return Failure(ElementNotFoundException(toSensString))
    }
    val newContext = context.addFrame
    newContext.setCurrentConcept(conceptDef)
    validateAndRemoveVariablePlaceholdersForConceptsChain(newContext, conceptsChain.tail)
  }

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    Some(this).filter(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    if (f(this)) List(this) else Nil
  }

  def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      this
    }
}
