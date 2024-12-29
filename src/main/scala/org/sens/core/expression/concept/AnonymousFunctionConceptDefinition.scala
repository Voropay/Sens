package org.sens.core.expression.concept

import org.sens.core.concept.{Annotation, Attribute, ParentConcept}
import org.sens.core.expression.SensExpression
import org.sens.core.statement.{SensStatement, StatementBlock}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.{Success, Try}

case class AnonymousFunctionConceptDefinition(body: SensStatement) extends SensConceptExpression {

  override def getName: String = throw new NotImplementedError()

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      body.findSubExpression(f)
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: body.findAllSubExpressions(f)
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConceptExpression =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case acd: SensConceptExpression => acd
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensConceptExpression")
      }
    } else {
      AnonymousFunctionConceptDefinition(body.replaceSubExpression(replaceSubExpression, withSubExpression))
    }

  override def getAttributeNames(context: ValidationContext): List[String] = throw new NotImplementedError()

  override def getAttributes(context: ValidationContext): List[Attribute] = throw new NotImplementedError()

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = throw new NotImplementedError()

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = throw new NotImplementedError()

  override def toSensString: String = body match {
    case x: StatementBlock => x.toSensString
    case x: SensStatement => "< " + x.toSensString + " >"
  }

  override def getAnnotations: List[Annotation] = List()

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConceptExpression] = {
    Try(AnonymousFunctionConceptDefinition(
      body.validateAndRemoveVariablePlaceholders(context.addFrame).get
    ))
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[SensConceptExpression] = Success(this)
}

