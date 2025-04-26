package org.sens.materializer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptSqlMaterializer
import org.sens.core.concept
import org.sens.core.concept.{Annotation, Attribute, Concept, DataSourceConcept, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.ConceptDefinition
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.FunctionCall
import org.sens.parser.ValidationContext

class ConceptSqlMaterializerTests extends AnyFlatSpec with Matchers {

  "Concept definition" should "be materialized into view" in {
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
      .annotations(Annotation.MATERIALIZED_VIEW :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef) should equal (
      """DROP VIEW IF EXISTS `conceptA`;
        |CREATE VIEW `conceptA` AS
        |SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`
        |FROM `someFile` AS `cb`
        |WHERE `val` = 1 AND `cb`.`attr4` > 0;""".stripMargin
    )
  }

  "Concept definition" should "be materialized into materialized view" in {
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
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(Annotation.TARGET_NAME -> StringLiteral("concept_a_table"), Annotation.TYPE -> StringLiteral(Annotation.MATERIALIZED_VIEW_TYPE))
      ) :: Nil
      ).build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef) should equal(
      """DROP MATERIALIZED VIEW IF EXISTS `concept_a_table`;
        |CREATE MATERIALIZED VIEW `concept_a_table` AS
        |SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`
        |FROM `someFile` AS `cb`
        |WHERE `val` = 1 AND `cb`.`attr4` > 0;""".stripMargin
    )
  }

  "Concept definition" should "be materialized into table" in {
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
      .annotations(Annotation.MATERIALIZED_TABLE :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef) should equal(
      """DROP TABLE IF EXISTS `conceptA`;
        |CREATE TABLE `conceptA` AS
        |SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`
        |FROM `someFile` AS `cb`
        |WHERE `val` = 1 AND `cb`.`attr4` > 0;""".stripMargin
    )
  }

  "Concept definition with dependency" should "be materialized with CTE" in {
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

    val conceptA = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("country", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("region", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .annotations(Annotation.MATERIALIZED_EPHEMERAL :: Nil)
      .build
    context.addConcept(ConceptDefinition(conceptA))

    val conDef = Concept.builder("conceptC",
      Attribute("region", Some(ConceptAttribute("ca" :: Nil, "region")), Nil) ::
        Attribute("val", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("ca" :: Nil, "val") :: Nil)),
          Nil
        ) :: Nil,
      ParentConcept(ConceptReference("conceptA"), Some("ca"), Map(), Nil) :: Nil)
      .attributeDependencies(
        Equals(ConceptAttribute("ca" :: Nil, "country"), StringLiteral("CA"))
      )
      .groupByAttributes(ConceptAttribute(Nil, "region") :: Nil)
      .annotations(Annotation.MATERIALIZED_TABLE :: Nil)
      .build

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef) should equal(
      """DROP TABLE IF EXISTS `conceptC`;
        |CREATE TABLE `conceptC` AS
        |WITH `conceptA` AS (SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` AS `val`, `cb`.`attr3` AS `country`, `cb`.`attr4` AS `region`
        |FROM `someFile` AS `cb`) (SELECT `ca`.`region` AS `region`, SUM(`ca`.`val`) AS `val`
        |FROM `conceptA` AS `ca`
        |WHERE `ca`.`country` = 'CA'
        |GROUP BY `region`);""".stripMargin
    )
    materializer.materializeSql(conceptA) should equal ("")
  }

  "Concept definition" should "be incrementally materialized as delete + insert" in {
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
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) ::
        Attribute("date", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "value"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(
          Annotation.TYPE -> StringLiteral(Annotation.INCREMENTAL_TYPE),
          Annotation.STRATEGY_TYPE -> StringLiteral(Annotation.DELETE_INSERT_STRATEGY),
          Annotation.UNIQUE_KEY -> StringLiteral("date"),
          Annotation.OPERATOR -> StringLiteral("="),
          Annotation.DEFAULT -> FunctionCall(FunctionReference("current_date"), Nil)
        )
      ) :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef, false) should equal(
      """DELETE FROM `conceptA`
        |WHERE `date` = CURRENT_DATE;
        |INSERT INTO `conceptA` (`id`, `value`, `date`)
        |(SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`, `cb`.`attr4` AS `date`
        |FROM `someFile` AS `cb`
        |WHERE `date` = CURRENT_DATE AND (`value` = 1 AND `cb`.`attr4` > 0));""".stripMargin
    )

    materializer.materializeSql(conDef) should equal(
      """DROP TABLE IF EXISTS `conceptA`;
        |CREATE TABLE `conceptA` AS
        |SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`, `cb`.`attr4` AS `date`
        |FROM `someFile` AS `cb`
        |WHERE `value` = 1 AND `cb`.`attr4` > 0;""".stripMargin
    )
  }

  "Concept definition" should "be incrementally materialized as append" in {
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
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) ::
        Attribute("date", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "value"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(
          Annotation.TYPE -> StringLiteral(Annotation.INCREMENTAL_TYPE),
          Annotation.STRATEGY_TYPE -> StringLiteral(Annotation.APPEND_STRATEGY),
          Annotation.UNIQUE_KEY -> StringLiteral("date"),
          Annotation.OPERATOR -> StringLiteral(">")
        )
      ) :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef, false, Some(IntLiteral(123456789))) should equal(
      """INSERT INTO `conceptA` (`id`, `value`, `date`)
        |(SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`, `cb`.`attr4` AS `date`
        |FROM `someFile` AS `cb`
        |WHERE `date` > 123456789 AND (`value` = 1 AND `cb`.`attr4` > 0));""".stripMargin
    )
  }

  "Concept definition" should "be incrementally materialized as merge" in {
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
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) ::
        Attribute("date", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "value"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(
          Annotation.TYPE -> StringLiteral(Annotation.INCREMENTAL_TYPE),
          Annotation.STRATEGY_TYPE -> StringLiteral(Annotation.MERGE_STRATEGY),
          Annotation.UNIQUE_KEY -> StringLiteral("date"),
          Annotation.OPERATOR -> StringLiteral(">")
        )
      ) :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef, false, Some(IntLiteral(123456789))) should equal(
      s"""MERGE INTO `conceptA` AS `sens_merge_target`
         |USING (SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`, `cb`.`attr4` AS `date`
         |FROM `someFile` AS `cb`
         |WHERE `date` > 123456789 AND (`value` = 1 AND `cb`.`attr4` > 0)) AS `sens_merge_source`
         |ON `sens_merge_source`.`date` = `sens_merge_target`.`date`
         |WHEN MATCHED THEN UPDATE SET `sens_merge_target`.`id` = `sens_merge_source`.`id`, `sens_merge_target`.`value` = `sens_merge_source`.`value`
         |WHEN NOT MATCHED THEN INSERT (`id`, `value`, `date`) (VALUES(`sens_merge_source`.`id`, `sens_merge_source`.`value`, `sens_merge_source`.`date`));""".stripMargin
    )
  }

  "Concept definition" should "be incrementally materialized as insert override merge" in {
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
        Attribute("value", Some(Multiply(ConceptAttribute(Nil, "amount"), ConceptAttribute(Nil, "price"))), Nil) ::
        Attribute("date", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute(Nil, "value"), IntLiteral(1)),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
      ))
      .annotations(new Annotation(
        Annotation.MATERIALIZED,
        Map(
          Annotation.TYPE -> StringLiteral(Annotation.INCREMENTAL_TYPE),
          Annotation.STRATEGY_TYPE -> StringLiteral(Annotation.INSERT_OVERRIDE_MERGE_STRATEGY),
          Annotation.UNIQUE_KEY -> StringLiteral("date"),
          Annotation.OPERATOR -> StringLiteral(">")
        )
      ) :: Nil)
      .build
    context.addConcept(ConceptDefinition(conDef))

    val materializer = ConceptSqlMaterializer.create(context)
    materializer.materializeSql(conDef, false, Some(IntLiteral(123456789))) should equal(
      s"""MERGE `conceptA` AS `sens_merge_target`
         |USING (SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` * `cb`.`attr3` AS `value`, `cb`.`attr4` AS `date`
         |FROM `someFile` AS `cb`
         |WHERE `date` > 123456789 AND (`value` = 1 AND `cb`.`attr4` > 0)) AS `sens_merge_source`
         |ON `sens_merge_source`.`date` = `sens_merge_target`.`date`
         |AND `sens_merge_target`.`date` > 123456789
         |WHEN NOT MATCHED BY TARGET THEN
         |INSERT (`id`, `value`, `date`)
         |VALUES(`sens_merge_source`.`id`, `sens_merge_source`.`value`, `sens_merge_source`.`date`)
         |WHEN MATCHED THEN UPDATE SET
         |(`sens_merge_target`.`id` = `sens_merge_source`.`id`, `sens_merge_target`.`value` = `sens_merge_source`.`value`)
         |WHEN NOT MATCHED BY SOURCE
         |AND `sens_merge_target`.`date` > 123456789
         |THEN DELETE;""".stripMargin
    )
  }
}
