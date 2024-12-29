package org.sens.converter

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptToRelConverter
import org.sens.core.concept.{Attribute, DataSourceConcept, IntersectConcept, MinusConcept, ParentConcept, UnionConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class UnionConceptsToRelTests extends AnyFlatSpec with Matchers {

  "Intersect Concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    val conDef1 = IntersectConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr1")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr2")), Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr3")), Nil) ::
            Attribute("attr4", Some(ConceptAttribute("c2" :: Nil, "attr4")), Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "attr5"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) ::
        Nil,
      Nil
    )

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT *\n" +
        "FROM `vs`.`conceptB`\n" +
      "INTERSECT ALL\n" +
        "SELECT `attr1`, `attr2`, `attr3`, `attr4`\n" +
        "FROM `vs`.`conceptC`\n" +
        "WHERE `attr5` = 'someType'"
    )
  }

  "Unions Concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    val conDef1 = UnionConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr1")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr2")), Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr3")), Nil) ::
            Attribute("attr4", Some(ConceptAttribute("c2" :: Nil, "attr4")), Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "attr5"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) ::
        Nil,
      Nil
    )

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT *\n" +
        "FROM `vs`.`conceptB`\n" +
        "UNION ALL\n" +
        "SELECT `attr1`, `attr2`, `attr3`, `attr4`\n" +
        "FROM `vs`.`conceptC`\n" +
        "WHERE `attr5` = 'someType'"
    )
  }

  "Minus Concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    val conDef1 = MinusConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr1")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr2")), Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr3")), Nil) ::
            Attribute("attr4", Some(ConceptAttribute("c2" :: Nil, "attr4")), Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "attr5"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) ::
        Nil,
      Nil
    )

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT *\n" +
        "FROM `vs`.`conceptB`\n" +
        "EXCEPT ALL\n" +
        "SELECT `attr1`, `attr2`, `attr3`, `attr4`\n" +
        "FROM `vs`.`conceptC`\n" +
        "WHERE `attr5` = 'someType'"
    )
  }

  def toSql(relNode: RelNode): String = {
    val sqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val sqlNode = sqlConverter.visitRoot(relNode).asStatement()
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }

}
