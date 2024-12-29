package org.sens.composer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.rel.ConceptSqlComposer
import org.sens.core.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext
import org.sens.core.expression.{ConceptAttribute, FunctionCall, GenericParameter}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.expression.operation.relational.Exists

class ConceptSqlComposerTests extends AnyFlatSpec with Matchers {

  "Concept definition without dependencies" should "be converted to sql without CTE" in {
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

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` + `cb`.`attr3` AS `val`\n" +
        "FROM `someFile` AS `cb`\n" +
        "WHERE `val` = 1 AND `cb`.`attr4` > 0"
    )
  }

  "Concept definition with dependency" should "be converted to sql with CTE" in {
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
      .build

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `conceptA` AS (SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` AS `val`, `cb`.`attr3` AS `country`, `cb`.`attr4` AS `region`\n" +
        "FROM `someFile` AS `cb`) " +
        "(SELECT `ca`.`region` AS `region`, SUM(`ca`.`val`) AS `val`\n" +
        "FROM `conceptA` AS `ca`\n" +
        "WHERE `ca`.`country` = 'CA'\n" +
        "GROUP BY `region`)"
    )
  }

  "Concept definition with materialized dependency" should "be converted to sql without CTE" in {
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
      .annotations(Annotation.MATERIALIZED_VIEW :: Nil)
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
      .build

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "SELECT `ca`.`region` AS `region`, SUM(`ca`.`val`) AS `val`\n" +
        "FROM `conceptA` AS `ca`\n" +
        "WHERE `ca`.`country` = 'CA'\n" +
        "GROUP BY `region`"
    )
  }

  "In concept definition CTEs" should "be topologically ordered" in {
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

    val conceptD = Concept.builder("conceptD",
      Attribute("region", Some(ConceptAttribute("ca" :: Nil, "region")), Nil) ::
        Attribute("val", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("ca" :: Nil, "val") :: Nil)),
          Nil
        ) :: Nil,
      ParentConcept(ConceptReference("conceptA"), Some("ca"), Map(), Nil) :: Nil)
      .attributeDependencies(
        Equals(ConceptAttribute("ca" :: Nil, "country"), StringLiteral("US"))
      )
      .groupByAttributes(ConceptAttribute(Nil, "region") :: Nil)
      .build
    context.addConcept(ConceptDefinition(conceptD))

    val conceptA = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("country", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("region", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build
    context.addConcept(ConceptDefinition(conceptA))

    val conceptC = Concept.builder("conceptC",
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
      .build
    context.addConcept(ConceptDefinition(conceptC))

    val conDef = Concept.builder("conceptE",
      Attribute("valCA", Some(ConceptAttribute("cc" :: Nil, "val")), Nil) ::
        Attribute("valUS", Some(ConceptAttribute("cd" :: Nil, "val")), Nil) ::
        Attribute("region", Some(ConceptAttribute("cd" :: Nil, "region")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map(), Nil) :: Nil)
      .attributeDependencies(
        Equals(ConceptAttribute("cc" :: Nil, "region"), ConceptAttribute("cd" :: Nil, "region"))
      )
      .build

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `conceptA` AS " +
        "(SELECT `cb`.`attr1` AS `id`, `cb`.`attr2` AS `val`, `cb`.`attr3` AS `country`, `cb`.`attr4` AS `region`\n" +
        "FROM `someFile` AS `cb`), " +
        "`conceptC` AS " +
        "(SELECT `ca`.`region` AS `region`, SUM(`ca`.`val`) AS `val`\n" +
        "FROM `conceptA` AS `ca`\n" +
        "WHERE `ca`.`country` = 'CA'\n" +
        "GROUP BY `region`), " +
        "`conceptD` AS " +
        "(SELECT `ca`.`region` AS `region`, SUM(`ca`.`val`) AS `val`\n" +
        "FROM `conceptA` AS `ca`\n" +
        "WHERE `ca`.`country` = 'US'\n" +
        "GROUP BY `region`) " +
        "(SELECT `cc`.`val` AS `valCA`, `cd`.`val` AS `valUS`, `cd`.`region` AS `region`\n" +
        "FROM `conceptC` AS `cc`\nINNER JOIN `conceptD` AS `cd` ON `cd`.`region` = `cc`.`region`)"
    )
  }

  "Parent concepts of anonymous concept definition" should "be converted to sql with CTE" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchase",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tableCustomer",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "purchase",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("customerId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("value", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchase"), Some("p"), Map(), Nil) :: Nil)
      .build))

    val conDef = Concept.builder(
      "customerTotalPurchase",
      Attribute("customerId", Some(ConceptAttribute("c" :: Nil, "attr1")), Nil) ::
        Attribute("email", Some(ConceptAttribute("c" :: Nil, "attr2")), Nil) ::
        Attribute("country", Some(ConceptAttribute("c" :: Nil, "attr3")), Nil) ::
        Attribute("dateRegistered", Some(ConceptAttribute("c" :: Nil, "attr4")), Nil) ::
        Attribute("totalValue", Some(ConceptAttribute("pp" :: Nil, "value")), Nil) :: Nil,
      ParentConcept(ConceptReference("tableCustomer"), Some("c"), Map(), Nil) ::
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("customerId", Some(ConceptAttribute("p" :: Nil, "customerId")), Nil) ::
              Attribute("value", Some(
                FunctionCall(FunctionReference("sum"), ConceptAttribute("p" :: Nil, "value") :: Nil)),
                Nil
              ) :: Nil,
            ParentConcept(ConceptReference("purchase"), Some("p"), Map(), Nil) :: Nil)
            .groupByAttributes(ConceptAttribute(Nil, "customerId") :: Nil)
            .build,
          Some("pp"),
          Map("customerId" -> ConceptAttribute("c" :: Nil, "attr1")),
          Nil) ::
        Nil)
      .attributeDependencies(
        GreaterThan(ConceptAttribute(List("c"), "attr4"), StringLiteral("2024-01-01"))
      )
      .build

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `purchase` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `customerId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`) " +
        "(SELECT `c`.`attr1` AS `customerId`, `c`.`attr2` AS `email`, `c`.`attr3` AS `country`, `c`.`attr4` AS `dateRegistered`, `pp`.`value` AS `totalValue`\n" +
        "FROM `someFile2` AS `c`\n" +
        "INNER JOIN (SELECT `p`.`customerId` AS `customerId`, SUM(`p`.`value`) AS `value`\n" +
        "FROM `purchase` AS `p`\n" +
        "GROUP BY `customerId`) AS `pp` ON `pp`.`customerId` = `c`.`attr1`\n" +
        "WHERE `c`.`attr4` > '2024-01-01')"
    )

    val conDef1 = Concept.builder(
      "customersWithPurchase",
      Attribute("customerId", Some(ConceptAttribute("c" :: Nil, "attr1")), Nil) ::
        Attribute("email", Some(ConceptAttribute("c" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("tableCustomer"), Some("c"), Map(), Nil) :: Nil
    ).attributeDependencies(
      Exists(AnonymousConceptDefinition.builder(
        Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) :: Nil,
        ParentConcept(ConceptReference("purchase"), Some("p"), Map(), Nil) :: Nil
      ).attributeDependencies(
        Equals(ConceptAttribute("p" :: Nil, "customerId"), ConceptAttribute("c" :: Nil, "attr1"))
      ).build())
    ).build()

    val conceptSql1 = conceptComposer.composeSelect(conDef1)
    conceptSql1 should equal(
      "WITH `purchase` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `customerId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`) " +
        "(SELECT `c`.`attr1` AS `customerId`, `c`.`attr2` AS `email`\n" +
        "FROM `someFile2` AS `c`\n" +
        "WHERE EXISTS (SELECT `p`.`id` AS `id`\n" +
        "FROM `purchase` AS `p`\n" +
        "WHERE `p`.`customerId` = `c`.`attr1`))"
    )

  }

  "Parent concepts of Intersect concept definition" should "be converted to sql with CTE"  in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchaseV1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchaseV2",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(Concept.builder(
      "purchaseV1",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("value", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchaseV1"), Some("p"), Map(), Nil) :: Nil)
      .build))
    context.addConcept(ConceptDefinition(Concept.builder(
      "purchaseV2",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("value", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchaseV2"), Some("p"), Map(), Nil) :: Nil)
      .build))

    val conceptComposer = ConceptSqlComposer.create(context)

    val conDef1 = IntersectConcept(
      "conceptIntersect",
      ParentConcept(ConceptReference("purchaseV1"), Some("pv1"), Map(), Nil) ::
        ParentConcept(ConceptReference("purchaseV2"), Some("pv2"), Map(), Nil) :: Nil,
      Nil
    )

    val conceptSql1 = conceptComposer.composeSelect(conDef1)
    conceptSql1 should equal(
      "WITH `purchaseV1` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`), " +
        "`purchaseV2` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile2` AS `p`) " +
        "(SELECT `pv1`.`id` AS `id`, `pv1`.`productId` AS `productId`, `pv1`.`value` AS `value`, `pv1`.`date` AS `date`\n" +
        "FROM `purchaseV1` AS `pv1`\n" +
        "INTERSECT\n" +
        "SELECT `pv2`.`id` AS `id`, `pv2`.`productId` AS `productId`, `pv2`.`value` AS `value`, `pv2`.`date` AS `date`\n" +
        "FROM `purchaseV2` AS `pv2`)"
    )

    val conDef2 = UnionConcept(
      "conceptUnion",
      ParentConcept(ConceptReference("purchaseV1"), Some("pv1"), Map(), Nil) ::
        ParentConcept(ConceptReference("purchaseV2"), Some("pv2"), Map(), Nil) :: Nil,
      Nil
    )

    val conceptSql2 = conceptComposer.composeSelect(conDef2)
    conceptSql2 should equal(
      "WITH `purchaseV1` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`), " +
        "`purchaseV2` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile2` AS `p`) " +
        "(SELECT `pv1`.`id` AS `id`, `pv1`.`productId` AS `productId`, `pv1`.`value` AS `value`, `pv1`.`date` AS `date`\n" +
        "FROM `purchaseV1` AS `pv1`\n" +
        "UNION\n" +
        "SELECT `pv2`.`id` AS `id`, `pv2`.`productId` AS `productId`, `pv2`.`value` AS `value`, `pv2`.`date` AS `date`\n" +
        "FROM `purchaseV2` AS `pv2`)"
    )

    val conDef3 = MinusConcept(
      "conceptMinus",
      ParentConcept(ConceptReference("purchaseV1"), Some("pv1"), Map(), Nil) ::
        ParentConcept(ConceptReference("purchaseV2"), Some("pv2"), Map(), Nil) :: Nil,
      Nil
    )

    val conceptSql3 = conceptComposer.composeSelect(conDef3)
    conceptSql3 should equal(
      "WITH `purchaseV1` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`), " +
        "`purchaseV2` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile2` AS `p`) " +
        "(SELECT `pv1`.`id` AS `id`, `pv1`.`productId` AS `productId`, `pv1`.`value` AS `value`, `pv1`.`date` AS `date`\n" +
        "FROM `purchaseV1` AS `pv1`\n" +
        "EXCEPT\n" +
        "SELECT `pv2`.`id` AS `id`, `pv2`.`productId` AS `productId`, `pv2`.`value` AS `value`, `pv2`.`date` AS `date`\n" +
        "FROM `purchaseV2` AS `pv2`)"
    )
  }

  "Parent concepts of Cube concept definition" should "be converted to sql with CTE"  in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someData",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) ::
        Attribute("attr6", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "someFact",
      Attribute("dim1", Some(ConceptAttribute("d" :: Nil, "attr1")), Nil) ::
        Attribute("dim2", Some(ConceptAttribute("d" :: Nil, "attr2")), Nil) ::
        Attribute("dim3", Some(ConceptAttribute("d" :: Nil, "attr3")), Nil) ::
        Attribute("val1", Some(ConceptAttribute("d" :: Nil, "attr4")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("d" :: Nil, "attr5")), Nil) ::
        Attribute("val3", Some(ConceptAttribute("d" :: Nil, "attr6")), Nil) :: Nil,
      ParentConcept(ConceptReference("someData"), Some("d"), Map(), Nil) :: Nil)
      .build))

    val conDef = CubeConcept.builder("metrics",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), StringLiteral("DISTINCT") :: ConceptAttribute("f" :: Nil, "val1") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("f" :: Nil, "val2") :: Nil)), Nil) ::
        Attribute("sum4", Some(FunctionCall(FunctionReference("sum"), StringLiteral("DISTINCT") :: ConceptAttribute("f" :: Nil, "val3") :: Nil)), Nil) :: Nil,
      Attribute("key1", Some(ConceptAttribute("f" :: Nil, "dim1")), Nil) ::
        Attribute("key2", Some(ConceptAttribute("f" :: Nil, "dim2")), Nil) :: Nil,
      ParentConcept(ConceptReference("someFact"), Some("f"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("f" :: Nil, "dim3"), IntLiteral(0)))
      .build()

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `someFact` AS (SELECT `d`.`attr1` AS `dim1`, `d`.`attr2` AS `dim2`, `d`.`attr3` AS `dim3`, " +
        "`d`.`attr4` AS `val1`, `d`.`attr5` AS `val2`, `d`.`attr6` AS `val3`\n" +
        "FROM `someFile1` AS `d`) " +
        "(SELECT " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `f`.`val1`) AS `count`, " +
        "SUM(`f`.`val2`) AS `sum3`, SUM(DISTINCT `f`.`val3`) AS `sum4`, " +
        "`f`.`dim1` AS `key1`, `f`.`dim2` AS `key2`\n" +
        "FROM `someFact` AS `f`\n" +
        "WHERE `f`.`dim3` > 0\n" +
        "GROUP BY `key1`, `key2`)"
    )
  }

  "Parent concepts of Cube Inherited concept definition" should "be converted to sql with CTE"  in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someData",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) ::
        Attribute("attr5", None, Nil) ::
        Attribute("attr6", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "someFact",
      Attribute("dim1", Some(ConceptAttribute("d" :: Nil, "attr1")), Nil) ::
        Attribute("dim2", Some(ConceptAttribute("d" :: Nil, "attr2")), Nil) ::
        Attribute("dim3", Some(ConceptAttribute("d" :: Nil, "attr3")), Nil) ::
        Attribute("val1", Some(ConceptAttribute("d" :: Nil, "attr4")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("d" :: Nil, "attr5")), Nil) ::
        Attribute("val3", Some(ConceptAttribute("d" :: Nil, "attr6")), Nil) :: Nil,
      ParentConcept(ConceptReference("someData"), Some("d"), Map(), Nil) :: Nil)
      .build))

    context.addConcept(ConceptDefinition(CubeConcept.builder("metrics",
      Attribute("total", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
        Attribute("count", Some(FunctionCall(FunctionReference("count"), StringLiteral("DISTINCT") :: ConceptAttribute("f" :: Nil, "val1") :: Nil)), Nil) ::
        Attribute("sum2", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("f" :: Nil, "val2") :: Nil)), Nil) ::
        Attribute("sum3", Some(FunctionCall(FunctionReference("sum"), StringLiteral("DISTINCT") :: ConceptAttribute("f" :: Nil, "val3") :: Nil)), Nil) :: Nil,
      Attribute("key1", Some(ConceptAttribute("f" :: Nil, "dim1")), Nil) ::
        Attribute("key2", Some(ConceptAttribute("f" :: Nil, "dim2")), Nil) :: Nil,
      ParentConcept(ConceptReference("someFact"), Some("f"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("f" :: Nil, "dim3"), IntLiteral(0)))
      .build()
    ))

    val conDef = CubeInheritedConcept.builder(
      "otherMetrics",
      ParentConcept(ConceptReference("metrics"), Some("m"), Map(), Nil))
      .overriddenMetrics(Attribute("avg2", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("m" :: "f" :: Nil, "val2") :: Nil)), Nil) :: Nil)
      .removedMetrics(ConceptAttribute("m" :: Nil, "sum3") :: Nil)
      .overriddenDimensions(Attribute("key3", Some(ConceptAttribute("m" :: "f" :: Nil, "dim3")), Nil) :: Nil)
      .removedDimensions(ConceptAttribute("m" :: Nil, "key2") :: Nil)
      .build()

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `someFact` AS (SELECT `d`.`attr1` AS `dim1`, `d`.`attr2` AS `dim2`, `d`.`attr3` AS `dim3`, " +
        "`d`.`attr4` AS `val1`, `d`.`attr5` AS `val2`, `d`.`attr6` AS `val3`\n" +
        "FROM `someFile1` AS `d`) " +
        "(SELECT " +
        "COUNT(*) AS `total`, COUNT(DISTINCT `f`.`val1`) AS `count`, " +
        "SUM(`f`.`val2`) AS `sum2`, AVG(`f`.`val2`) AS `avg2`, " +
        "`f`.`dim1` AS `key1`, `f`.`dim3` AS `key3`\n" +
        "FROM `someFact` AS `f`\n" +
        "WHERE `f`.`dim3` > 0\n" +
        "GROUP BY `key1`, `key3`)"
    )
  }

  "Parent concepts of Inherited concept definition" should "be converted to sql with CTE"  in {
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

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `purchaseV1` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `value`, `p`.`attr4` AS `date`, `p`.`attr5` AS `tax`\n" +
        "FROM `someFile1` AS `p`) " +
        "(SELECT `pv1`.`id` AS `id`, `pv1`.`productId` AS `productId`, `pv1`.`date` AS `date`, `pv1`.`value` + `pv1`.`tax` AS `value`\n" +
        "FROM `purchaseV1` AS `pv1`\n" +
        "WHERE `pv1`.`date` > '2000/01/01')"
    )
  }

  "Parent concepts of Aggregation concept definition" should "be converted to sql with CTE" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchase",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tableProduct",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "purchase",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("amount", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("tablePurchase"), Some("p"), Map(), Nil) :: Nil)
      .build))

    context.addConcept(ConceptDefinition(Concept.builder(
      "product",
      Attribute("id", Some(ConceptAttribute("t" :: Nil, "attr1")), Nil) ::
        Attribute("name", Some(ConceptAttribute("t" :: Nil, "attr2")), Nil) ::
        Attribute("price", Some(ConceptAttribute("t" :: Nil, "attr3")), Nil) ::
        Attribute("description", Some(ConceptAttribute("t" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("tableProduct"), Some("t"), Map(), Nil) :: Nil)
      .build))

    val conDef = AggregationConcept.builder(
      "productPurchased",
      ParentConcept(ConceptReference("purchase"), Some("prc"), Map(), Nil) ::
        ParentConcept(ConceptReference("product"), Some("prd"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("prd" :: Nil, "id"), ConceptAttribute("prc" :: Nil, "productId")))
      .build

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `purchase` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `amount`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`), " +
        "`product` AS (SELECT `t`.`attr1` AS `id`, `t`.`attr2` AS `name`, `t`.`attr3` AS `price`, `t`.`attr4` AS `description`\n" +
        "FROM `someFile2` AS `t`) " +
        "(SELECT ROW(`prc`.`id`, `prc`.`productId`, `prc`.`amount`, `prc`.`date`) AS `prc`, " +
        "ROW(`prd`.`id`, `prd`.`name`, `prd`.`price`, `prd`.`description`) AS `prd`\n" +
        "FROM `purchase` AS `prc`\nINNER JOIN `product` AS `prd` ON `prd`.`id` = `prc`.`productId`)"
    )
  }

  "Parent concepts of Generic Concept definition" should "be converted to sql with CTE" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "tablePurchase",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(Concept.builder(
      "purchase",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
        Attribute("productId", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::
        Attribute("amount", Some(ConceptAttribute("p" :: Nil, "attr3")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ExpressionIdent(GenericParameter("source")), Map()),
        Some("p"), Map(), Nil) :: Nil)
      .genericParameters("source" :: Nil)
      .build()))

    val conDef = Concept.builder(
      "purchaseStats",
      Attribute("total", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("p" :: Nil, "amount") :: Nil)), Nil) ::
        Attribute("date", None, Nil) :: Nil,
      ParentConcept(GenericConceptReference(ConstantIdent("purchase"), Map("source" -> StringLiteral("tablePurchase"))), Some("p"), Map(), Nil) :: Nil
    ).groupByAttributes(ConceptAttribute(Nil, "date") :: Nil).build()

    val conceptComposer = ConceptSqlComposer.create(context)
    val conceptSql = conceptComposer.composeSelect(conDef)
    conceptSql should equal(
      "WITH `_generic_purchase_tablePurchase` AS (SELECT `p`.`attr1` AS `id`, `p`.`attr2` AS `productId`, `p`.`attr3` AS `amount`, `p`.`attr4` AS `date`\n" +
        "FROM `someFile1` AS `p`) " +
        "(SELECT " +
        "SUM(`p`.`amount`) AS `total`, " +
        "`p`.`date` AS `date`\n" +
        "FROM `_generic_purchase_tablePurchase` AS `p`\n" +
        "GROUP BY `date`)"
    )

  }




}
