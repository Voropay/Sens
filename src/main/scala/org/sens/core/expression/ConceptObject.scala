package org.sens.core.expression

import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.{Failure, Success, Try}

case class ConceptObject(name: String) extends SensExpression {
  override def toSensString: String = name

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    if(!context.containsParentConcept(name))
      Failure(ElementNotFoundException(name))
    else
      Success(this)
  }

  def getAttributeNames(context: ValidationContext): List[String] = {
    if(!context.containsParentConcept(name)) {
      throw ElementNotFoundException(name)
    }
    val parentConcept = context.getParentConcept(name).get
    parentConcept.concept.getAttributeNames(context)
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
