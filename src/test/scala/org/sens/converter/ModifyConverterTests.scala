package org.sens.converter

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptToRelConverter
import org.sens.core.concept.{Attribute, Concept, DataSourceConcept, ParentConcept}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.literal.IntLiteral
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.logical.And

class ModifyConverterTests extends AnyFlatSpec with Matchers {

  "Delete" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .build
    context.addConcept(ConceptDefinition(conDef))

    val condition = Equals(ConceptAttribute(Nil, "id"), IntLiteral(1))
    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toDeleteRel("conceptA", condition)
    toSql(conceptRel) should equal(
      "DELETE " +
        "FROM `vs`.`conceptA`\n" +
        "WHERE `id` = 1"
    )
  }

  def toSql(relNode: RelNode): String = {
    val sqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val sqlNode = sqlConverter.visitRoot(relNode).asStatement()
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }
}
