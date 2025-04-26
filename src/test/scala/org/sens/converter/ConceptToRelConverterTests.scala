package org.sens.converter

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptToRelConverter
import org.sens.core.concept.{AggregationConcept, Annotation, Attribute, Concept, CubeConcept, CubeInheritedConcept, DataSourceConcept, FunctionConcept, InheritedConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.{ConceptAttribute, FunctionCall}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression.literal.{BooleanLiteral, IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.{ConceptDefinition, NOP}
import org.sens.parser.ValidationContext

class ConceptToRelConverterTests extends AnyFlatSpec with Matchers {

  "Basic concept definition" should "be converted to relational algebra correctly" in {
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

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0"
    )
  }

  "Concept definition with joins" should "be converted to relational algebra correctly" in {
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
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptD",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile3", FileFormats.CSV),
      Nil
    )))

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("cc" :: Nil, "attr1"), ConceptAttribute("cb" :: Nil, "attr1")),
        And(
          Equals(ConceptAttribute("cd" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1")),
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        )
      ))
      .build

    val conceptRel3 = conceptConverter.toRel(conDef3.inferAttributeExpressions(context).get)
    toSql(conceptRel3) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef4 = Concept.builder("conceptA",
      Attribute("left", Some(ConceptAttribute("c1" :: Nil, "attr1")), Nil) ::
        Attribute("right", Some(ConceptAttribute("c2" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptB"), Some("c2"), Map("attr1" -> ConceptAttribute("c1" :: Nil, "attr2")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("c1" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel4 = conceptConverter.toRel(conDef4)
    toSql(conceptRel4) should equal (
      "SELECT `conceptB`.`attr1` AS `left`, `conceptB0`.`attr1` AS `right`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN `vs`.`conceptB` AS `conceptB0` ON `conceptB`.`attr2` = `conceptB0`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )
  }

  "Concept definition with optional parent concepts" should "be converted to relational algebra correctly" in {
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
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptD",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile3", FileFormats.CSV),
      Nil
    )))

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "LEFT JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "LEFT JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Annotation.OPTIONAL :: Nil) ::
        ParentConcept(ConceptReference("conceptC"),  Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel3 = conceptConverter.toRel(conDef3)
    toSql(conceptRel3) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "RIGHT JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef4 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Annotation.OPTIONAL :: Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel4 = conceptConverter.toRel(conDef4)
    toSql(conceptRel4) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "FULL JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef5 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(
        ConceptReference("conceptB"),
        Some("cb"), Map(),
        Annotation.OPTIONAL :: Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) ::
        ParentConcept(
          ConceptReference("conceptD"),
          Some("cd"),
          Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel5 = conceptConverter.toRel(conDef5)
    toSql(conceptRel5) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "FULL JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "FULL JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

    val conDef6 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(
        ConceptReference("conceptB"),
        Some("cb"), Map(),
        Annotation.OPTIONAL :: Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Annotation.OPTIONAL :: Nil) ::
        ParentConcept(
          ConceptReference("conceptD"),
          Some("cd"),
          Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")),
          Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptRel6 = conceptConverter.toRel(conDef6)
    toSql(conceptRel6) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `conceptB`.`attr2` + `conceptB`.`attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "FULL JOIN `vs`.`conceptC` ON `conceptB`.`attr1` = `conceptC`.`attr1`\n" +
        "RIGHT JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "WHERE `conceptB`.`attr4` > 0"
    )

  }

  "Concept definition with aggregates" should "be converted to relational algebra correctly" in {
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

    val conDef1 = Concept.builder("conceptA",
      Attribute("key1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("key2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), BooleanLiteral(true) :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .groupByAttributes(ConceptAttribute(Nil, "key1") :: ConceptAttribute(Nil, "key2")  :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1`, `attr2`, " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `attr2`) AS `count`, " +
        "SUM(`attr3`) AS `sum3`, SUM(DISTINCT `attr4`) AS `sum4`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0\n" +
        "GROUP BY `attr1`, `attr2`"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), BooleanLiteral(true) :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .groupByAttributes(Add(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cb" :: Nil, "attr2"))  :: Nil)
      .groupDependencies(GreaterThan(ConceptAttribute(Nil, "sum3"), IntLiteral(10)))
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `attr1` + `attr2` AS `$f4`, " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `attr2`) AS `count`, " +
        "SUM(`attr3`) AS `sum3`, SUM(DISTINCT `attr4`) AS `sum4`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0\n" +
        "GROUP BY `attr1` + `attr2`\n" +
        "HAVING SUM(`attr3`) > 10"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptRel3 = conceptConverter.toRel(conDef3)
    toSql(conceptRel3) should equal(
      "SELECT COUNT(*) AS `total`\n" +
        "FROM `vs`.`conceptB`"
    )

    val conDef4 = Concept.builder("conceptA",
      Attribute("category", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
      .groupDependencies(GreaterThan(FunctionCall(FunctionReference("count"), Nil), IntLiteral(1)))
      .build

    val conceptRel4 = conceptConverter.toRel(conDef4)
    toSql(conceptRel4) should equal(
      "SELECT `attr1`\n" +
        "FROM `vs`.`conceptB`\n" +
        "GROUP BY `attr1`\n" +
        "HAVING COUNT(*) > 1"
    )
  }

  "Concept definition with nested anonymous concept definition" should "be converted to relational algebra correctly" in {
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
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptD",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile3", FileFormats.CSV),
      Nil
    )))

    val conDef = Concept.builder(
      "conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("attr1", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
              Attribute("attr2", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
            ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
              ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
              Nil)
            .build,
          Some("cc"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Nil) ::
        Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
        Equals(ConceptAttribute(List(), "id"), IntLiteral(1))))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal (
      "SELECT `conceptB`.`attr1` AS `id`, `t`.`attr2` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN (SELECT `conceptC`.`attr1`, `conceptC`.`attr2`\n" +
        "FROM `vs`.`conceptC`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`" +
        ") AS `t` ON `conceptB`.`attr1` = `t`.`attr1`\n" +
        "WHERE `conceptB`.`attr3` = 'SomeValue' AND `conceptB`.`attr1` = 1"
    )

    val conDef1 = Concept.builder(
      "conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("ccd" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("attr2", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cc" :: Nil, "attr2") :: Nil)), Nil) ::
              Attribute("attr1", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil,
            ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
              ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
              Nil)
            .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
            .build,
          Some("ccd"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Nil) ::
        Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
        Equals(ConceptAttribute(List(), "id"), IntLiteral(1))))
      .build

    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal(
      "SELECT `conceptB`.`attr1` AS `id`, `t`.`attr2` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "INNER JOIN (SELECT `conceptC`.`attr1`, SUM(`conceptC`.`attr2`) AS `attr2`\n" +
        "FROM `vs`.`conceptC`\n" +
        "INNER JOIN `vs`.`conceptD` ON `conceptC`.`attr1` = `conceptD`.`attr1`\n" +
        "GROUP BY `conceptC`.`attr1`" +
        ") AS `t` ON `conceptB`.`attr1` = `t`.`attr1`\n" +
        "WHERE `conceptB`.`attr3` = 'SomeValue' AND `conceptB`.`attr1` = 1"
    )
  }

  "Concept definition with limit and offset" should "be converted to relational algebra correctly" in {
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

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .limit(10)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .offset(10)
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0\n" +
        "OFFSET 10 ROWS"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .limit(10)
      .offset(100)
      .build

    val conceptRel3 = conceptConverter.toRel(conDef3)
    toSql(conceptRel3) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0\n" +
        "OFFSET 100 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Concept definition with sorting" should "be converted to relational algebra correctly" in {
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

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .orderByAttributes(
        Order(ConceptAttribute(Nil, "val"), Order.ASC) ::
          Order(ConceptAttribute(Nil, "id"), Order.DESC) :: Nil
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0\n" +
        "ORDER BY `attr2` + `attr3`, `attr1` DESC"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr4"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      )
      .orderByAttributes(
        Order(Add(ConceptAttribute(Nil, "attr2"), ConceptAttribute(Nil, "attr3")), Order.ASC) ::
          Order(ConceptAttribute(Nil, "val"), Order.ASC) :: Nil
      )
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `attr1` AS `id`, `attr2`, `attr3`, `attr2` + `attr4` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0\n" +
        "ORDER BY `attr2` + `attr3`, `attr2` + `attr4`"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .orderByAttributes(
        Order(ConceptAttribute(Nil, "val"), Order.ASC) ::
          Order(ConceptAttribute(Nil, "id"), Order.DESC) :: Nil
      )
      .limit(10)
      .offset(20)
      .build

    val conceptRel3 = conceptConverter.toRel(conDef3)
    toSql(conceptRel3) should equal (
      "SELECT `attr1` AS `id`, `attr2` + `attr3` AS `val`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr2` + `attr3` = 1 AND `attr4` > 0\n" +
        "ORDER BY `attr2` + `attr3`, `attr1` DESC\n" +
        "OFFSET 20 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Concept definition with ephemeral attributes" should "be converted to relational algebra correctly" in {
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
        Attribute("amount", Some(ConceptAttribute("cb" :: Nil, "attr2")), Annotation.PRIVATE :: Nil) ::
        Attribute("price", Some(ConceptAttribute("cb" :: Nil, "attr3")), Annotation.PRIVATE :: Nil) ::
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `attr1` AS `id`, `attr2` * `attr3` AS `value`\n" +
        "FROM `vs`.`conceptB`"
    )
  }

  "Concept definition with function concept call" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(FunctionConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Annotation.INPUT :: Nil) ::
        Attribute("attr3", None, Annotation.INPUT :: Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      Nil,
      AnonymousFunctionDefinition(Nil, NOP()),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptC",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef1 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("cat1", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("cat2", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr2" -> IntLiteral(1), "attr3" -> StringLiteral("someVal")), Nil) :: Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal (
      "SELECT `attr1` AS `id`, `attr2` AS `cat1`, `attr3` AS `cat2`, `attr4` AS `val`\n" +
        "FROM (SELECT *\n" +
        "FROM TABLE(`conceptB`(1, 'someVal'))) AS `t`\n" +
        "WHERE `attr4` > 0"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
        ParentConcept(
          ConceptReference("conceptB"),
          Some("cb"),
          Map("attr2" -> IntLiteral(1), "attr3" -> StringLiteral("someVal"), "attr1" -> ConceptAttribute("cc" :: Nil, "attr1")),
          Nil) :: Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      )
      .build

    val conceptRel2 = conceptConverter.toRel(conDef2)
    toSql(conceptRel2) should equal (
      "SELECT `conceptC`.`attr1` AS `id`, `t`.`attr4` AS `val`\n" +
        "FROM `vs`.`conceptC`\n" +
        "INNER JOIN (SELECT *\n" +
        "FROM TABLE(`conceptB`(1, 'someVal'))) AS `t` ON `conceptC`.`attr1` = `t`.`attr1`\n" +
        "WHERE `t`.`attr4` > 0"
    )
  }

  "Concept definition with Unique annotation" should "be converted to relational algebra correctly" in {
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
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .annotations(Annotation.UNIQUE :: Nil)
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `attr1` AS `id`\n" +
        "FROM `vs`.`conceptB`\n" +
        "GROUP BY `attr1`"
    )
  }

  "Cube Concept definition" should "be converted to relational algebra correctly" in {
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

    val conDef1 = CubeConcept.builder("conceptA",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), BooleanLiteral(true) :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      Attribute("key1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("key2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel1 = conceptConverter.toRel(conDef1)
    toSql(conceptRel1) should equal(
      "SELECT `attr1`, `attr2`, " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `attr2`) AS `count`, " +
        "SUM(`attr3`) AS `sum3`, SUM(DISTINCT `attr4`) AS `sum4`\n" +
        "FROM `vs`.`conceptB`\n" +
        "WHERE `attr4` > 0\n" +
        "GROUP BY `attr1`, `attr2`"
    )
  }

  "Cube Inherited Concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someConcept",
      Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(
      CubeConcept.builder(
        "someCube",
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
        Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil)
        .annotations(Annotation("Private", Map()) :: Nil).build()
    ))

    val conDef = CubeInheritedConcept(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
      Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "category") :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: "c" :: Nil, "date"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(ConceptAttribute("c" :: Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute("c" :: Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Nil
    )

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `date`, " +
        "SUM(`val`) AS `totalVal`, MAX(`val`) AS `maxVal`\n" +
        "FROM `vs`.`someConcept`\n" +
        "WHERE `date` > '2024-01-01'\n" +
        "GROUP BY `date`\n" +
        "HAVING SUM(`val`) > 100\n" +
        "ORDER BY SUM(`val`)\n" +
        "OFFSET 100 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Inherited concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchaseV1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "purchaseV1",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("value", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) ::
        Attribute("tax", Some(ConceptAttribute("p" :: Nil, "attr5")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchaseV1"), Some("p"), Map(), Nil) :: Nil)
      .build))

    val conDef = InheritedConcept.builder("purchase",
      ParentConcept(ConceptReference("purchaseV1"), Some("pv1"), Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("value", Some(Add(
        ConceptAttribute("pv1" :: Nil, "value"),
        ConceptAttribute("pv1" :: Nil, "tax"))), Nil) :: Nil)
      .removedAttributes(ConceptAttribute("pv1" :: Nil, "tax") :: Nil)
      .additionalDependencies(
        GreaterThan(ConceptAttribute("pv1" :: Nil, "date"), StringLiteral("2000/01/01"))
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `id`, `productId`, `date`, `value` + `tax` AS `value`\n" +
        "FROM `vs`.`purchaseV1`\n" +
        "WHERE `date` > '2000/01/01'"
    )
  }

  "Aggregation concept definition" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchase",
      Attribute("id", None, Nil) ::
        Attribute("productId", None, Nil) ::
        Attribute("amount", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tableProduct",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("description", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    val conDef = AggregationConcept.builder(
      "productPurchased",
      ParentConcept(ConceptReference("tablePurchase"), Some("prc"), Map(), Nil) ::
        ParentConcept(ConceptReference("tableProduct"), Some("prd"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("prd" :: Nil, "id"), ConceptAttribute("prc" :: Nil, "productId")))
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT ROW(`tablePurchase`.`id`, `tablePurchase`.`productId`, `tablePurchase`.`amount`, `tablePurchase`.`date`) AS `prc`, " +
        "ROW(`tableProduct`.`id`, `tableProduct`.`name`, `tableProduct`.`price`, `tableProduct`.`description`) AS `prd`\n" +
        "FROM `vs`.`tablePurchase`\n" +
        "INNER JOIN `vs`.`tableProduct` ON `tablePurchase`.`productId` = `tableProduct`.`id`"
    )
  }

  "Concept definition with naterialization target annotation" should "be converted to relational algebra correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchaseV1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "purchaseV1",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("value", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) ::
        Attribute("tax", Some(ConceptAttribute("p" :: Nil, "attr5")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchaseV1"), Some("p"), Map(), Nil) :: Nil)
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(Annotation.TYPE -> StringLiteral(Annotation.TABLE_TYPE), Annotation.TARGET_NAME -> StringLiteral("purchase_v1"))) :: Nil)
      .build))

    val conDef = InheritedConcept.builder("purchase",
      ParentConcept(ConceptReference("purchaseV1"), Some("pv1"), Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("value", Some(Add(
        ConceptAttribute("pv1" :: Nil, "value"),
        ConceptAttribute("pv1" :: Nil, "tax"))), Nil) :: Nil)
      .removedAttributes(ConceptAttribute("pv1" :: Nil, "tax") :: Nil)
      .additionalDependencies(
        GreaterThan(ConceptAttribute("pv1" :: Nil, "date"), StringLiteral("2000/01/01"))
      )
      .build

    val conceptConverter = ConceptToRelConverter.create(context)
    val conceptRel = conceptConverter.toRel(conDef)
    toSql(conceptRel) should equal(
      "SELECT `id`, `productId`, `date`, `value` + `tax` AS `value`\n" +
        "FROM `vs`.`purchase_v1`\n" +
        "WHERE `date` > '2000/01/01'"
    )
  }

  def toSql(relNode: RelNode): String = {
    val sqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val sqlNode = sqlConverter.visitRoot(relNode).asStatement()
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }

}
