package org.sens.converter.optimization.concept

import org.sens.core.concept._
import org.sens.core.expression.SensExpression
import org.sens.core.expression.concept.{AnonymousConceptDefinition, SensConceptExpression}
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.operation.relational.Exists
import org.sens.parser.ValidationContext

object ConvertExistsToParentConceptRule extends ConvertRelationalExpressionToParentConceptRule[Exists] {

  def apply(concept: SensConcept, context: ValidationContext): SensConcept = {
    concept match {
      case c: Concept => convertConcept(c, context)
      case _ => concept
    }
  }

  def generateAttributeName(concept: AnonymousConceptDefinition): String = {
    val base = "exists_attribute_"
    var i = 1;
    while (true) {
      val name = base + i
      if (concept.attributes.exists(_.name == name)) {
        i += 1
      } else {
        return name
      }
    }
    base
  }

  def replaceAttributes(concept: SensConceptExpression): (SensConceptExpression, String) = {
    concept match {
      case acd: AnonymousConceptDefinition =>
        val attrName = generateAttributeName(acd)
        val attributes = Attribute(attrName, Some(BooleanLiteral(true)), Nil) :: acd.attributes
        val conDef = acd.copy(attributes = attributes)
        (conDef, attrName)
      case other =>
        val attrName = "exists_attribute"
        val conDef = AnonymousConceptDefinition.builder(
          Attribute(attrName, Some(BooleanLiteral(true)), Nil) :: Nil,
          ParentConcept(other, None, Map(), Nil) :: Nil
        ).build()
        (conDef, attrName)
    }
  }

  override def prepareParentConcept(existsExpr: Exists, alias: String, isExprInRootAnd: Boolean, context: ValidationContext): (ParentConcept, String) = {
    val acd = existsExpr.operand
    val (acdWithoutAttributes, attrName) = replaceAttributes(acd)

    val parentConcept = ParentConcept(
      acdWithoutAttributes,
      Some(alias),
      Map(),
      if (!isExprInRootAnd) Annotation.OPTIONAL :: Nil else Nil
    )
    (parentConcept, attrName)
  }

  override def ifExprIsRelOp(expr: SensExpression): Boolean =
    expr match {
      case _: Exists => true
      case _ => false
    }
}
