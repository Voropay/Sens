package org.sens.converter.optimization.concept

import org.sens.core.concept._
import org.sens.core.expression.{SensExpression}
import org.sens.core.expression.operation.relational.InSubQuery
import org.sens.parser.ValidationContext

object ConvertInToParentConceptRule extends ConvertRelationalExpressionToParentConceptRule[InSubQuery] {

  def apply(concept: SensConcept, context: ValidationContext): SensConcept = {
    concept match {
      case c: Concept => convertConcept(c, context)
      case _ => concept
    }
  }

  override def prepareParentConcept(inExpr: InSubQuery, alias: String, isExprInRootAnd: Boolean, context: ValidationContext): (ParentConcept, String) = {
    val acd = inExpr.operand2
    val attrName = acd.getAttributes(context).head.name

    val parentConcept = ParentConcept(
      acd,
      Some(alias),
      Map(attrName -> inExpr.operand1),
      if (!isExprInRootAnd) Annotation.OPTIONAL :: Nil else Nil
    )
    (parentConcept, attrName)
  }

  override def ifExprIsRelOp(expr: SensExpression): Boolean =
    expr match {
      case _: InSubQuery => true
      case _ => false
    }


}
