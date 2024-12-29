package org.sens.core.expression

import org.sens.core.concept.ParentConcept
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.parser.{ElementNotFoundException, FirstClassCitizens, ValidationContext, ValidationException}

import scala.util.{Failure, Success, Try}

case class NamedElementPlaceholder(name: String) extends SensExpression {

  override def toSensString: String = name

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    val element = context.find(name)
    if(element.isEmpty) {
      Failure(ElementNotFoundException(name))
    } else {
      element.get._1 match {
        case FirstClassCitizens.Variable => Success(Variable(name))
        case FirstClassCitizens.Function => Success(FunctionReference(name))
        case FirstClassCitizens.StandardSensFunction => Success(FunctionReference(name))
        case FirstClassCitizens.Concept => Success(ConceptReference(name))
        case FirstClassCitizens.Attribute => Success(ConceptAttribute(Nil, name))
        case FirstClassCitizens.ParentConcept => Success(ConceptObject(name))
        case FirstClassCitizens.ParentConceptAttribute =>
          if(element.get._2.size == 1) {
            val parentConcept = element.get._2.head.asInstanceOf[ParentConcept]
            Success(ConceptAttribute(parentConcept.getAlias :: Nil, name))
          } else {
            Failure(new ValidationException("Attribute " + name + " defined in more than one parent concept"))
          }
        case FirstClassCitizens.GenericParameter => Success(GenericParameter(name))
        case FirstClassCitizens.GenericParentConceptAttribute => Success(this)
      }
    }
  }

  override def getSubExpressions: List[SensExpression] = Nil

  def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = Some(this).filter(f)

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
