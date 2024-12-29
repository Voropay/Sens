package org.sens.converter.optimization.concept

import org.sens.core.concept.{Annotation, Concept, ParentConcept, SensConcept}
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.core.expression.concept.AnonymousConceptDefinition
import org.sens.core.expression.literal.{BooleanLiteral, NullLiteral}
import org.sens.core.expression.operation.SensOperation
import org.sens.core.expression.operation.comparison.{Equals, NotEquals}
import org.sens.core.expression.operation.logical.{And, AndSeq, Not}
import org.sens.parser.ValidationContext

trait ConvertRelationalExpressionToParentConceptRule[T <: SensOperation] {
  def generateAlias(concept: Concept): String = {
    val base = "acd_exists_"
    var i = 1;
    while (true) {
      val alias = base + i
      if (concept.parentConcepts.exists(_.getAlias == alias)) {
        i += 1
      } else {
        return alias
      }
    }
    base
  }

  def addParentConcept(concept: SensConcept, pc: ParentConcept): SensConcept =
    concept match {
      case c: Concept =>
        val newParentConcepts = c.parentConcepts ::: (pc :: Nil)
        c.copy(parentConcepts = newParentConcepts)
      case acd: AnonymousConceptDefinition =>
        val newParentConcepts = acd.parentConcepts ::: (pc :: Nil)
        acd.copy(parentConcepts = newParentConcepts)
      case other => other
    }

  def addUniqueAnnotation(concept: SensConcept): SensConcept = {
    val withUniqueAnnotation = if(concept.getAnnotations.contains(Annotation.UNIQUE)) {
      concept.getAnnotations
    } else {
      Annotation.UNIQUE :: concept.getAnnotations
    }
    concept match {
      case c: Concept => c.copy(annotations = withUniqueAnnotation)
      case acd: AnonymousConceptDefinition => acd //TODO: add unique annotation
      case other => other
    }
  }

  def convertConcept(concept: Concept, context: ValidationContext): SensConcept = {
    val notRelOp = concept.findSubExpression {
      case not: Not => ifExprIsRelOp(not.operand)
      case _ => false
    }
    val isNotRelOp = notRelOp.isDefined

    val relOp = if (notRelOp.isDefined) {
      None
    } else {
      concept.findSubExpression(ifExprIsRelOp)
    }
    if (notRelOp.isEmpty && relOp.isEmpty) {
      return concept
    }
    val relOpExpr = if (notRelOp.isDefined) {
      notRelOp.get.asInstanceOf[Not].operand.asInstanceOf[T]
    } else {
      relOp.get.asInstanceOf[T]
    }

    val isExprInRootAnd = if (isNotRelOp) {
      false
    } else {
      if (concept.attributeDependencies.isDefined) {
        concept.attributeDependencies.get match {
          case And(op1, op2) => op1 == relOpExpr || op2 == relOpExpr
          case AndSeq(ops) => ops.contains(relOpExpr)
          case exp if exp == relOpExpr => true
          case _ => false
        }
      } else {
        false
      }
    }

    val alias = generateAlias(concept)

    val (parentConcept, attrName) = prepareParentConcept(relOpExpr, alias, isExprInRootAnd, context)

    val conceptWithoutRelOp = if (isNotRelOp) {
      concept.replaceSubExpression(Not(relOpExpr), Equals(ConceptAttribute(alias :: Nil, attrName), NullLiteral()))
    } else if (isExprInRootAnd) {
      concept.replaceSubExpression(relOpExpr, BooleanLiteral(true))
    } else {
      concept.replaceSubExpression(relOpExpr, NotEquals(ConceptAttribute(alias :: Nil, attrName), NullLiteral()))
    }


    val relOpInParentConcept = addParentConcept(conceptWithoutRelOp, parentConcept)
    if(isNotRelOp) {
      relOpInParentConcept
    } else {
      addUniqueAnnotation(relOpInParentConcept)
    }
  }

  def prepareParentConcept(inExpr: T, alias: String, isExprInRootAnd: Boolean, context: ValidationContext): (ParentConcept, String)

  def ifExprIsRelOp(expr: SensExpression): Boolean
}
