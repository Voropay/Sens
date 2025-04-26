package org.sens.converter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptToSqlConverter
import org.sens.core.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.statement.{ConceptDefinition, NOP}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, WindowFunction, WindowFunctions}
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression.operation.relational._
import org.sens.parser.ValidationContext

class ConceptToSqlConverterTests extends AnyFlatSpec with Matchers {
  "Basic concept definition" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0"
    )
  }

  "Concept definition with joins" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "INNER JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0")

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build
    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "INNER JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
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
    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "INNER JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
    )

    val conDef4 = Concept.builder("conceptA",
      Attribute("left", Some(ConceptAttribute("c1" :: Nil, "attr1")), Nil) ::
        Attribute("right", Some(ConceptAttribute("c2" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptB"), Some("c2"), Map("attr1" -> ConceptAttribute("c1" :: Nil, "attr2")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("c1" :: Nil, "attr4"), IntLiteral(0)))
      .build
    val conceptSql4 = converter.toSql(conDef4)
    conceptSql4 should equal(
      "SELECT `c1`.`attr1` AS `left`, `c2`.`attr1` AS `right`\n" +
        "FROM `someFile1` AS `c1`\n" +
        "INNER JOIN `someFile1` AS `c2` ON `c2`.`attr1` = `c1`.`attr2`\n" +
        "WHERE `c1`.`attr4` > 0"
    )
  }

  "Concept definition with optional parent concepts" should "be converted to sql correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal (
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "LEFT JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
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

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "LEFT JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(Add(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute("cb" :: Nil, "attr3"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Annotation.OPTIONAL :: Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "RIGHT JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
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

    val conceptSql4 = converter.toSql(conDef4)
    conceptSql4 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "FULL JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
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

    val conceptSql5 = converter.toSql(conDef5)
    conceptSql5 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "FULL JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "FULL JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
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

    val conceptSql6 = converter.toSql(conDef6)
    conceptSql6 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "FULL JOIN `someFile2` AS `cc` ON `cc`.`attr1` = `cb`.`attr1`\n" +
        "RIGHT JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
    )
  }

  "Concept definition with aggregates" should "be converted to SQL correctly" in {
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
        Attribute("count", Some(FunctionCall(FunctionReference("count"), StringLiteral("DISTINCT") :: ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), StringLiteral("DISTINCT") :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .groupByAttributes(ConceptAttribute(Nil, "key1") :: ConceptAttribute(Nil, "key2") :: Nil)
      .build

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `key1`, `cb`.`attr2` AS `key2`, " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `cb`.`attr2`) AS `count`, " +
        "SUM(`cb`.`attr3`) AS `sum3`, SUM(DISTINCT `cb`.`attr4`) AS `sum4`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "WHERE `cb`.`attr4` > 0\n" +
        "GROUP BY `key1`, `key2`"
    )

    val conDef2 = Concept.builder("conceptA",
      Attribute("key", Some(Add(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cb" :: Nil, "attr2"))), Nil) ::
        Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), StringLiteral("DISTINCT") :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .groupByAttributes(ConceptAttribute(Nil, "key") :: Nil)
      .groupDependencies(GreaterThan(ConceptAttribute(Nil, "sum3"), IntLiteral(10)))
      .build

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cb`.`attr1` + `cb`.`attr2` AS `key`, " +
        "COUNT(*) AS `total`, COUNT(`cb`.`attr2`) AS `count`, " +
        "SUM(`cb`.`attr3`) AS `sum3`, SUM(DISTINCT `cb`.`attr4`) AS `sum4`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "WHERE `cb`.`attr4` > 0\n" +
        "GROUP BY `key`\n" +
        "HAVING `sum3` > 10"
    )

    val conDef3 = Concept.builder("conceptA",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
      .build

    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT COUNT(*) AS `total`\n" +
        "FROM `someFile1` AS `conceptB`"
    )

    val conDef4 = Concept.builder("conceptA",
      Attribute("category", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
      .groupDependencies(GreaterThan(FunctionCall(FunctionReference("count"), Nil), IntLiteral(1)))
      .build

    val conceptSql4 = converter.toSql(conDef4)
    conceptSql4 should equal(
      "SELECT `cb`.`attr1` AS `category`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "GROUP BY `category`\n" +
        "HAVING COUNT(*) > 1"
    )
  }

  "Concept definition with nested anonymous concept definition" should "be converted to SQL correctly" in {
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
        Attribute("val", Some(ConceptAttribute("ccd" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("attr1", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
              Attribute("attr2", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
            ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
              ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
              Nil)
            .build,
          Some("ccd"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")),
          Nil) ::
        Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
        Equals(ConceptAttribute(List(), "id"), IntLiteral(1))))
      .build

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT `cb`.`attr1` AS `id`, `ccd`.`attr2` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "INNER JOIN (SELECT `cc`.`attr1` AS `attr1`, `cc`.`attr2` AS `attr2`\n" +
        "FROM `someFile2` AS `cc`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`" +
        ") AS `ccd` ON `ccd`.`attr1` = `cb`.`attr1`\n" +
        "WHERE `cb`.`attr3` = 'SomeValue' AND `id` = 1"
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

    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `ccd`.`attr2` AS `val`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "INNER JOIN (SELECT SUM(`cc`.`attr2`) AS `attr2`, `cc`.`attr1` AS `attr1`\n" +
        "FROM `someFile2` AS `cc`\n" +
        "INNER JOIN `someFile3` AS `cd` ON `cd`.`attr1` = `cc`.`attr1`\n" +
        "GROUP BY `attr1`" +
        ") AS `ccd` ON `ccd`.`attr1` = `cb`.`attr1`\n" +
        "WHERE `cb`.`attr3` = 'SomeValue' AND `id` = 1"
    )
  }

  "Concept definition with relational expressions" should "be converted to SQL correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "products",
      Attribute("id", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("category", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "categories",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    val converter = ConceptToSqlConverter.create(context)

    val conDef1 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(InSubQuery(
      ConceptAttribute("p" :: Nil, "category"),
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
        .build)
    ).build
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `p`.`id` AS `id`, `p`.`price` AS `price`, `p`.`category` AS `category`\n" +
        "FROM `someFile1` AS `p`\n" +
        "WHERE `p`.`category` IN (" +
        "SELECT `c`.`id` AS `id`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE `c`.`parentCategory` = 1)"
    )

    val conDef2 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(Any(GreaterThan(
      ConceptAttribute("p" :: Nil, "category"),
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
        .build)
    )).build

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `p`.`id` AS `id`, `p`.`price` AS `price`, `p`.`category` AS `category`\n" +
        "FROM `someFile1` AS `p`\n" +
        "WHERE `p`.`category` > SOME (" +
        "SELECT `c`.`id` AS `id`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE `c`.`parentCategory` = 1)"
    )

    val conDef3 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(All(GreaterThan(
      ConceptAttribute("p" :: Nil, "category"),
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "parentCategory"), IntLiteral(1)))
        .build)
    )).build

    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT `p`.`id` AS `id`, `p`.`price` AS `price`, `p`.`category` AS `category`\n" +
        "FROM `someFile1` AS `p`\n" +
        "WHERE `p`.`category` > ALL (" +
        "SELECT `c`.`id` AS `id`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE `c`.`parentCategory` = 1)"
    )

    val conDef4 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(Exists(
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("p" :: Nil, "category")))
        .build)
    ).build

    val conceptSql4 = converter.toSql(conDef4)
    conceptSql4 should equal(
      "SELECT `p`.`id` AS `id`, `p`.`price` AS `price`, `p`.`category` AS `category`\n" +
        "FROM `someFile1` AS `p`\n" +
        "WHERE EXISTS (" +
        "SELECT `c`.`id` AS `id`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE `c`.`id` = `p`.`category`)"
    )

    val conDef5 = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("price", Some(ConceptAttribute("p" :: Nil, "price")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).attributeDependencies(Unique(
      AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("categories"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("p" :: Nil, "category")))
        .build)
    ).build

    val conceptSql5 = converter.toSql(conDef5)
    conceptSql5 should equal(
      "SELECT `p`.`id` AS `id`, `p`.`price` AS `price`, `p`.`category` AS `category`\n" +
        "FROM `someFile1` AS `p`\n" +
        "WHERE UNIQUE (" +
        "SELECT `c`.`id` AS `id`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE `c`.`id` = `p`.`category`)"
    )

  }

  "Concept definition with limit and offset" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0\n" +
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

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0\n" +
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

    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0\n" +
        "OFFSET 100 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Concept definition with sorting" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0\n" +
        "ORDER BY `val`, `id` DESC"
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

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` AS `attr2`, `cb`.`attr3` AS `attr3`, `cb`.`attr2` + `cb`.`attr4` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `cb`.`attr4` > 0\n" +
        "ORDER BY `attr2` + `attr3`, `val`"
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

    val conceptSql3 = converter.toSql(conDef3)
    conceptSql3 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0\n" +
        "ORDER BY `val`, `id` DESC\n" +
        "OFFSET 20 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Concept definition with ephemeral attributes" should "be converted to SQL correctly" in {
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
        Attribute("amount", Some(ConceptAttribute("cb" :: Nil, "attr2")), Annotation.PRIVATE :: Nil) ::
        Attribute("price", Some(ConceptAttribute("cb" :: Nil, "attr3")), Annotation.PRIVATE :: Nil) ::
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`\n" +
        "FROM `someFile` AS `cb`"
    )
  }

  "Concept definition with function concept call" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` AS `cat1`, `cb`.`attr3` AS `cat2`, `cb`.`attr4` AS `val`\n" +
        "FROM `conceptB`(1, 'someVal') AS `cb`\n" +
        "WHERE `cb`.`attr4` > 0"
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

    val conceptSql2 = converter.toSql(conDef2)
    conceptSql2 should equal(
      "SELECT `cc`.`attr1` AS `id`, `cb`.`attr4` AS `val`\n" +
        "FROM `someFile` AS `cc`\n" +
        "INNER JOIN `conceptB`(1, 'someVal') AS `cb` ON `cb`.`attr1` = `cc`.`attr1`\n" +
        "WHERE `cb`.`attr4` > 0"
    )
  }

  "Concept definition with Unique annotation" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT DISTINCT `cb`.`attr1` AS `id`\n" +
        "FROM `someFile` AS `cb`"
    )
  }

  "Cube Concept definition" should "be converted to SQL correctly" in {
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
        Attribute("count", Some(FunctionCall(FunctionReference("count"), StringLiteral("DISTINCT") :: ConceptAttribute("cb" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("cb" :: Nil, "attr3") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), StringLiteral("DISTINCT") :: ConceptAttribute("cb" :: Nil, "attr4") :: Nil)), Nil) :: Nil,
      Attribute("key1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("key2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)))
      .build

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql1 = converter.toSql(conDef1)
    conceptSql1 should equal(
      "SELECT " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `cb`.`attr2`) AS `count`, " +
        "SUM(`cb`.`attr3`) AS `sum3`, SUM(DISTINCT `cb`.`attr4`) AS `sum4`, " +
        "`cb`.`attr1` AS `key1`, `cb`.`attr2` AS `key2`\n" +
        "FROM `someFile1` AS `cb`\n" +
        "WHERE `cb`.`attr4` > 0\n" +
        "GROUP BY `key1`, `key2`"
    )
  }

  "Cube Inherited Concept definition" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT " +
        "SUM(`c`.`val`) AS `totalVal`, MAX(`c`.`val`) AS `maxVal`, `c`.`date` AS `date`\n" +
        "FROM `someFile` AS `c`\n" +
        "WHERE `c`.`date` > '2024-01-01'\n" +
        "GROUP BY `date`\n" +
        "HAVING `totalVal` > 100\n" +
        "ORDER BY `totalVal`\n" +
        "OFFSET 100 ROWS\n" +
        "FETCH NEXT 10 ROWS ONLY"
    )
  }

  "Inherited concept definition" should "be converted to SQL correctly" in {
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

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT `pv1`.`id` AS `id`, `pv1`.`productId` AS `productId`, `pv1`.`date` AS `date`, `pv1`.`value` + `pv1`.`tax` AS `value`\n" +
        "FROM `purchaseV1` AS `pv1`\n" +
        "WHERE `pv1`.`date` > '2000/01/01'"
    )
  }

  "Aggregation concept definition" should "be converted to SQL correctly" in {
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
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    val conDef = AggregationConcept.builder(
      "productPurchased",
      ParentConcept(ConceptReference("tablePurchase"), Some("prc"), Map(), Nil) ::
        ParentConcept(ConceptReference("tableProduct"), Some("prd"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("prd" :: Nil, "id"), ConceptAttribute("prc" :: Nil, "productId")))
      .build

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT ROW(`prc`.`id`, `prc`.`productId`, `prc`.`amount`, `prc`.`date`) AS `prc`, " +
        "ROW(`prd`.`id`, `prd`.`name`, `prd`.`price`, `prd`.`description`) AS `prd`\n" +
        "FROM `someFile1` AS `prc`\nINNER JOIN `someFile2` AS `prd` ON `prd`.`id` = `prc`.`productId`"
    )
  }

  "Concept Attributes definition" should "be converted to SQL correctly" in {
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

    val conDef = ConceptAttributes.builder(
      "purchaseDimensions",
      Attribute("productId", Some(ConceptAttribute("p" :: Nil, "productId")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "date")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchase"), Some("p"), Map(), Nil) :: Nil
    ).build()

    val converter = ConceptToSqlConverter.create(context)
    val conceptSql = converter.toSql(conDef)
    conceptSql should equal(
      "SELECT `p`.`productId` AS `productId`, `p`.`date` AS `date`\n" +
        "FROM `someFile1` AS `p`"
    )
  }

  "Concept definition with window functions" should "be converted to SQL correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "products",
      Attribute("id", None, Nil) ::
        Attribute("price", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))
    val converter = ConceptToSqlConverter.create(context)

    val conDef = Concept.builder(
      "someProducts",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("priceAvg", Some(
          WindowFunction(
            WindowFunctions.AVG,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            Nil,
            Nil,
            (None, None),
            (None, None)
          )
        ), Nil) ::
        Attribute("priceSumByCategory", Some(
          WindowFunction(
            WindowFunctions.SUM,
            ConceptAttribute("p" :: Nil, "price") :: Nil,
            ConceptAttribute("p" :: Nil, "category") :: Nil,
            Nil,
            (None, None),
            (None, None)
          )
        ), Nil) ::
        Nil,
      ParentConcept(ConceptReference("products"), Some("p"), Map(), Nil) :: Nil
    ).build()

    val conceptSql = converter.toSql(conDef)
    conceptSql should equal (
      "SELECT `p`.`id` AS `id`, " +
        "AVG(`p`.`price`) OVER () AS `priceAvg`, " +
        "SUM(`p`.`price`) OVER (PARTITION BY `p`.`category`) AS `priceSumByCategory`\n" +
        "FROM `someFile` AS `p`"
      )

  }

}
