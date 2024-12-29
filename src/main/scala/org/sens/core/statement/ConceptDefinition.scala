package org.sens.core.statement

import org.sens.core.DataModelElement
import org.sens.core.concept.SensConcept
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class ConceptDefinition(concept: SensConcept) extends SensStatement with DataModelElement {
  override def toSensString: String = concept.toSensString

  override def getSubExpressions: List[SensExpression] = Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    val validatedDefinition = Try(ConceptDefinition(
      concept.validateAndRemoveVariablePlaceholders(context).get
    ))
    if(validatedDefinition.isSuccess) {
      context.addConcept(this)
    }
    validatedDefinition
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = concept.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    concept.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement = {
    ConceptDefinition(concept.replaceSubExpression(replaceSubExpression, withSubExpression))
  }
}
