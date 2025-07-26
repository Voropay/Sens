package org.sens.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression
import org.sens.core.expression.{ConceptAttribute, FunctionCall, NamedElementPlaceholder, literal}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, AnonymousFunctionConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression.literal.{FloatTypeLiteral, IntLiteral, IntTypeLiteral, StringLiteral, StringTypeLiteral}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.Not
import org.sens.core.expression.operation.relational.Exists
import org.sens.core.statement._

import scala.collection.immutable.Map

class ConceptsParserTest extends AnyFlatSpec with Matchers {
  val conceptParser = new SensParser

  "Annotations" should "be parsed correctly" in {
    conceptParser.parse(conceptParser.annotationParser, "@NotNull").get should equal (Annotation("NotNull", Map()))
    conceptParser.parse(
      conceptParser.annotationParser,
      "@Owner (name = \"Oleksii Voropai\", email=\"oleksii.voropai@email.org\")"
    ).get should equal (
      Annotation(
        "Owner",
        Map("name" -> StringLiteral("Oleksii Voropai"), "email" -> StringLiteral("oleksii.voropai@email.org"))
      )
    )

    conceptParser.parse(
      conceptParser.annotationParser,
      "@Default (value = current_date())"
    ).get should equal(
      Annotation(
        "Default",
        Map("value" -> FunctionCall(FunctionReference("current_date"), Nil))
      )
    )

  }

  "Attributes" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "@NotNull, @Default(value = \"\")  myAttr"
    ).get should equal (
      Attribute(
        "myAttr",
        None,
        Annotation("NotNull", Map()) ::
        Annotation("Default", Map("value" ->StringLiteral(""))) :: Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "myAttr"
    ).get should equal (
      Attribute(
        "myAttr",
        None,
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "myAttr = 1"
    ).get should equal (
      Attribute(
        "myAttr",
        Some(IntLiteral(1)),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "@PrimaryKey string(36) userId"
    ).get should equal(
      Attribute(
        "userId",
        None,
        Annotation("PrimaryKey", Map()) :: Nil,
        Some(StringTypeLiteral(36))
      )
    )

    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "myMetrics[source: \"s\", sourceType: \"metricsList\"]"
    ).get should equal(
      ConceptAttributesReference(
        ConstantIdent("myMetrics"),
        Map("source" -> StringLiteral("s"), "sourceType" -> StringLiteral("metricsList")))
    )

    conceptParser.parse(
      conceptParser.sensAttributeParser,
      "$metrics[source: \"s\"]"
    ).get should equal(
      ConceptAttributesReference(
        ExpressionIdent(NamedElementPlaceholder("metrics")),
        Map("source" -> StringLiteral("s")))
    )
  }

  "Parent concepts" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.parentConceptParser,
      "@Optional, @Relation(name = \"belongsTo\")  myParentConcept mpc"
    ).get should equal (
      ParentConcept(
        ConceptReference("myParentConcept"),
        Some("mpc"),
        Map(),
        Annotation("Optional", Map()) ::
        Annotation("Relation", Map("name" ->StringLiteral("belongsTo"))) :: Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "myParentConcept mpc"
    ).get should equal (
      ParentConcept(
        ConceptReference("myParentConcept"),
        Some("mpc"),
        Map(),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "myParentConcept"
    ).get should equal (
      ParentConcept(
        ConceptReference("myParentConcept"),
        None,
        Map(),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "myParentConcept mpc (id = c1.parentId, type = \"someType\")"
    ).get should equal (
      ParentConcept(
        ConceptReference("myParentConcept"),
        Some("mpc"),
        Map("id" -> ConceptAttribute("c1" :: Nil, "parentId"), "type" -> StringLiteral("someType")),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "myParentConcept (id = c1.parentId)"
    ).get should equal (
      ParentConcept(
        ConceptReference("myParentConcept"),
        None,
        Map("id" -> ConceptAttribute("c1" :: Nil, "parentId")),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "myParentConcept[param: \"value\"] (id = c1.parentId)"
    ).get should equal(
      ParentConcept(
        GenericConceptReference(ConstantIdent("myParentConcept"), Map("param" -> StringLiteral("value"))),
        None,
        Map("id" -> ConceptAttribute("c1" :: Nil, "parentId")),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.parentConceptParser,
      "$sourceConcept[param: \"value\"] (id = c1.parentId)"
    ).get should equal(
      ParentConcept(
        GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("sourceConcept")), Map("param" -> StringLiteral("value"))),
        None,
        Map("id" -> ConceptAttribute("c1" :: Nil, "parentId")),
        Nil
      )
    )
  }

  "Order expressions" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.orderParser,
      "myAttr asc"
    ).get should equal (
      Order(
        ConceptAttribute(Nil, "myAttr"),
        Order.ASC
      )
    )

    conceptParser.parse(
      conceptParser.orderParser,
      "myAttr desc"
    ).get should equal (
      Order(
        ConceptAttribute(Nil, "myAttr"),
        Order.DESC
      )
    )

    conceptParser.parse(
      conceptParser.orderParser,
      "c1.myAttr asc"
    ).get should equal (
      Order(
        ConceptAttribute("c1" :: Nil ,"myAttr"),
        Order.ASC
      )
    )
  }

  "Anonymous concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.conceptExpression,
      "<from myconcept>"
    ).get should equal (
      AnonymousConceptDefinition.builder(
        List(),
        ParentConcept(ConceptReference("myconcept"), None, Map(), Nil) :: Nil)
      .build()
    )

    conceptParser.parse(
      conceptParser.conceptExpression,
      "<(attr1, attr2) from myconcept>"
    ).get should equal (
      AnonymousConceptDefinition.builder(
        Attribute("attr1", None, List()) :: Attribute("attr2", None, List()) :: Nil,
        ParentConcept(ConceptReference("myconcept"), None, Map(), Nil) :: Nil)
        .build
    )

    conceptParser.parse(
      conceptParser.conceptExpression,
      "<(attr1 = c1.val, attr2 = c2.val) from myconcept1 c1, myconcept2 c2 where c1.id = c2.c1Id>"
    ).get should equal (
      AnonymousConceptDefinition.builder(
        Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), List()) ::
        Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "val")), List()) :: Nil,
        ParentConcept(ConceptReference("myconcept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("myconcept2"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "c1Id")))
      .build
    )

    conceptParser.parse(
      conceptParser.conceptExpression,
      "<@Unique() (attr1 = c1.val, attr2 = c2.val) from myconcept1 c1 (id = c2.c1Id), myconcept2 c2>"
    ).get should equal (
      AnonymousConceptDefinition.builder(
        Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), List()) ::
        Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "val")), List()) :: Nil,
        ParentConcept(ConceptReference("myconcept1"), Some("c1"), Map("id" -> ConceptAttribute("c2" :: Nil, "c1Id")), Nil) ::
        ParentConcept(ConceptReference("myconcept2"), Some("c2"), Map(), Nil) :: Nil)
        .annotations(Annotation.UNIQUE :: Nil)
        .build
    )
  }

  "Anonymous function concept expression" should "be parsed correctly" in {
    conceptParser.parse(
    conceptParser.conceptExpression,
    "< return c.attr1 >"
    ).get should equal (
      AnonymousFunctionConceptDefinition(
        Return(Some(ConceptAttribute("c" :: Nil, "attr1")))
      )
    )
  }

  "Concept reference expression" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.conceptExpression,
      "concept1"
    ).get should equal (ConceptReference("concept1"))
  }

  "Concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept (attr1 = c.attr1, attr2 = c.attr2) from parentConcept c"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("attr1", Some(ConceptAttribute("c" :: Nil, "attr1")), List()) ::
          Attribute("attr2", Some(ConceptAttribute("c" :: Nil, "attr2")), List()) :: Nil,
        ParentConcept(ConceptReference("parentConcept"), Some("c"), Map(), Nil) :: Nil)
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept (attr1 = c1.val, attr2 = c2.val)\n" +
        "from parentConcept1 c1, parentConcept2 c2\n" +
        "where c1.id = c2.c2Ref"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), List()) ::
          Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "val")), List()) :: Nil,
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) ::
          ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "c2Ref")))
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept (attr1 = c1.val, attr2 = c2.val)\n" +
        "from parentConcept1 c1, parentConcept2 c2\n" +
        "where c1.id = c2.c2Ref"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), List()) ::
          Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "val")), List()) :: Nil,
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) ::
          ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "c2Ref")))
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")" +
      "concept myConcept (@NotNull attr1 = c1.val, attr2 = sum(c2.val))\n" +
      "from @Relation(name = \"someRel1\") parentConcept1 c1 (id = c2.c1Ref), @Relation(name = \"someRel2\") parentConcept2 c2\n" +
      "group by attr1 having attr2 > 100 order by attr2 desc limit 10 offset 20"
    ).get should equal (
      Concept(
        "myConcept",
        Nil,
        Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Annotation("NotNull", Map()) :: Nil) ::
        Attribute("attr2", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("c2" :: Nil, "val") :: Nil)),
          Nil
        ) :: Nil,
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map("id" -> ConceptAttribute("c2" :: Nil, "c1Ref")), Annotation("Relation", Map("name" -> StringLiteral("someRel1"))) :: Nil) ::
        ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Annotation("Relation", Map("name" -> StringLiteral("someRel2"))) :: Nil) :: Nil,
        None,
        NamedElementPlaceholder("attr1") :: Nil,
        Some(GreaterThan(NamedElementPlaceholder("attr2"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "attr2"), Order.DESC) :: Nil,
        Some(10),
        Some(20),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Generic Concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept[sourceName, sourceParam] (key = s.attr1, val = s.attr2)\n" +
        "from $sourceName[suffix: sourceParam] s"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("key", Some(ConceptAttribute("s" :: Nil, "attr1")), List()) ::
          Attribute("val", Some(ConceptAttribute("s" :: Nil, "attr2")), List()) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("sourceName")), Map("suffix" -> NamedElementPlaceholder("sourceParam"))),
          Some("s"), Map(), Nil) :: Nil)
        .genericParameters("sourceName" :: "sourceParam" :: Nil)
        .build
    )
  }

  "Concept definition with concept attributes reference" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept (" +
        "key = s.attr1, " +
        "myMetrics[s: \"s\", sourceType: \"mySource\"]" +
        ")\n" +
        "from mySource s"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("key", Some(ConceptAttribute("s" :: Nil, "attr1")), List()) ::
          ConceptAttributesReference(
            ConstantIdent("myMetrics"),
            Map("s" -> StringLiteral("s"), "sourceType" -> StringLiteral("mySource"))) :: Nil,
        ParentConcept(
          ConceptReference("mySource"),
          Some("s"), Map(), Nil) :: Nil)
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept [sourceName] (" +
        "key = s.attr1, " +
        "myMetrics[s: \"s\", sourceType: sourceName]" +
        ")\n" +
        "from $sourceName s"
    ).get should equal(
      Concept.builder(
        "myConcept",
        Attribute("key", Some(ConceptAttribute("s" :: Nil, "attr1")), List()) ::
          ConceptAttributesReference(
            ConstantIdent("myMetrics"),
            Map("s" -> StringLiteral("s"), "sourceType" -> NamedElementPlaceholder("sourceName"))) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("sourceName")), Map()),
          Some("s"), Map(), Nil) :: Nil)
        .genericParameters("sourceName" :: Nil)
        .build
    )
  }

  "Cube Concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")" +
        "concept cube myCube metrics (totalVal = sum(c.val)) dimensions (@NotNull dim = c.dim)\n" +
        "from someFactConcept c\n" +
        "where c.otherDim = 1" +
        "having totalVal > 100 order by dim desc limit 10 offset 20"
    ).get should equal(
      CubeConcept(
        "myCube",
        Attribute("totalVal", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)),
          Nil
        ) :: Nil,
        Attribute("dim", Some(ConceptAttribute("c" :: Nil, "dim")), Annotation("NotNull", Map()) :: Nil) :: Nil,
        ParentConcept(ConceptReference("someFactConcept"), Some("c"), Map(), Nil) :: Nil,
        Some(Equals(ConceptAttribute("c" :: Nil, "otherDim"), IntLiteral(1))),
        Some(GreaterThan(NamedElementPlaceholder("totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "dim"), Order.DESC) :: Nil,
        Some(10),
        Some(20),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Materialized (type = \"Table\")\n" +
        "concept cube orderMetrics\n" +
        "metrics (ordersCount = count(), priceSum = sum(price_total))\n" +
        "dimensions (order_date, status)" +
        "from order"
    ).get should equal (
      CubeConcept.builder(
        "orderMetrics",
        Attribute("ordersCount", Some(FunctionCall(FunctionReference("count"), Nil)), Nil) ::
          Attribute("priceSum", Some(FunctionCall(FunctionReference("sum"), NamedElementPlaceholder("price_total") :: Nil)), Nil) :: Nil,
        Attribute("order_date", None, Nil) :: Attribute("status", None, Nil) :: Nil,
        ParentConcept(ConceptReference("order"), None, Map(), Nil) :: Nil)
        .annotations(Annotation(Annotation.MATERIALIZED, Map(Annotation.TYPE -> StringLiteral(Annotation.TABLE_TYPE))) :: Nil)
        .build()
    )

  }

  "Cube Inherited Concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")" +
        "concept cube myCube is someCubeConcept c\n" +
        "with metrics totalVal = sum(c.c.val), avgVal = avg(c.c.val)\n" +
        "without metrics c.cnt\n" +
        "with dimensions @NotNull category = c.c.category, country = c.c.country\n" +
        "without dimensions c.date\n" +
        "where c.date = \"2024-01-01\"" +
        "having totalVal > 100 order by category desc limit 10 offset 20"
    ).get should equal(
      CubeInheritedConcept(
        "myCube",
        ParentConcept(ConceptReference("someCubeConcept"), Some("c"), Map(), Nil),
        Attribute("totalVal", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)),
          Nil
        ) ::
          Attribute("avgVal", Some(
            FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)),
            Nil
          ) :: Nil,
        ConceptAttribute("c" :: Nil, "cnt") :: Nil,
        Attribute("category", Some(ConceptAttribute("c" :: "c" :: Nil, "category")), Annotation("NotNull", Map()) :: Nil) ::
          Attribute("country", Some(ConceptAttribute("c" :: "c" :: Nil, "country")), Nil) :: Nil,
        ConceptAttribute("c" :: Nil, "date") :: Nil,
        Some(Equals(ConceptAttribute("c" :: Nil, "date"), StringLiteral("2024-01-01"))),
        Some(GreaterThan(NamedElementPlaceholder("totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "category"), Order.DESC) :: Nil,
        Some(10),
        Some(20),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Cube Concept definition with ConceptAttributesReference" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
        "concept cube myCube metrics (" +
        "metricsList [src: \"s\"]" +
        ") dimensions (" +
        "dimensionsList [src: \"s\"]" +
        ")\n" +
        "from someFactConcept c"
    ).get should equal(
      CubeConcept.builder(
        "myCube",
        ConceptAttributesReference(ConstantIdent("metricsList"), Map("src" -> StringLiteral("s"))) :: Nil,
        ConceptAttributesReference(ConstantIdent("dimensionsList"), Map("src" -> StringLiteral("s"))) :: Nil,
        ParentConcept(ConceptReference("someFactConcept"), Some("c"), Map(), Nil) :: Nil
      ).build()
    )
  }

  "Inherited concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept is parentConcept"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept"), None, Map(), Nil) :: Nil)
      .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept is parentConcept with type = \"someType\" without attr1, attr2"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept"), None, Map(), Nil) :: Nil)
        .overriddenAttributes(Attribute("type", Some(StringLiteral("someType")), Nil) :: Nil)
        .removedAttributes(ConceptAttribute(Nil, "attr1") :: ConceptAttribute(Nil, "attr2") :: Nil)
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept is parentConcept where type = \"someType\""
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept"), None, Map(), Nil) :: Nil)
        .additionalDependencies(Equals(NamedElementPlaceholder("type"), StringLiteral("someType")))
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept is parentConcept order by val asc limit 10"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept"), None, Map(), Nil) :: Nil)
        .orderByAttributes(Order(ConceptAttribute(Nil, "val"), Order.ASC) :: Nil)
        .limit(10)
        .build
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept is parentConcept1 c1, parentConcept2 c2 without c1.id where c1.id = c2.c1Ref"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Nil) :: Nil)
        .removedAttributes(ConceptAttribute("c1" :: Nil, "id") :: Nil)
        .additionalDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "c1Ref")))
        .build
    )
  }

  "Generic Inherited Concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept[sourceName, sourceParam] is $sourceName[suffix: sourceParam] s with key = s.attr1, val = s.attr2"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(
          GenericConceptReference(ExpressionIdent(NamedElementPlaceholder("sourceName")), Map("suffix" -> NamedElementPlaceholder("sourceParam"))),
          Some("s"), Map(), Nil) :: Nil)
        .overriddenAttributes(Attribute("key", Some(ConceptAttribute("s" :: Nil, "attr1")), List()) ::
          Attribute("val", Some(ConceptAttribute("s" :: Nil, "attr2")), List()) :: Nil)
        .genericParameters("sourceName" :: "sourceParam" :: Nil)
        .build
    )
  }

  "Aggregation concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\") concept myConcept between parentConcept1 c1, parentConcept2 c2 where c1.parentId = c2.id"
    ).get should equal(
      AggregationConcept(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Nil) :: Nil,
        Some(Equals(ConceptAttribute("c1" :: Nil, "parentId"), ConceptAttribute("c2" :: Nil, "id"))),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept between parentConcept1 c1, parentConcept2 c2 (id = c1.parentId)"
    ).get should equal(
      AggregationConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map("id" -> ConceptAttribute("c1" :: Nil, "parentId")), Nil) :: Nil)
        .build
    )
  }

  "Concept attributes definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")" +
        "concept attributes myMetrics (totalVal = sum(s.val), avgVal = avg(s.val))\n" +
        "from sourceConcept s"
    ).get should equal(
      ConceptAttributes(
        "myMetrics",
        Nil,
        Attribute("totalVal", Some(
          FunctionCall(FunctionReference("sum"), ConceptAttribute("s" :: Nil, "val") :: Nil)),
          Nil
        ) ::
          Attribute("avgVal", Some(
            FunctionCall(FunctionReference("avg"), ConceptAttribute("s" :: Nil, "val") :: Nil)),
            Nil
          ) :: Nil,
        ParentConcept(ConceptReference("sourceConcept"), Some("s"), Map(),  Nil) :: Nil,
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Function concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\") concept myConcept (attr1, attr2) from parentConcept1 by () => read_csv_file(\"in.csv\")"
    ).get should equal(
      FunctionConcept(
        "myConcept",
        Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("parentConcept1"), None, Map(), Nil) :: Nil,
        AnonymousFunctionDefinition(
          Nil,
          ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
        ),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept from parentConcept1 by () => read_csv_file(\"in.csv\")"
    ).get should equal(
      FunctionConcept(
        "myConcept",
        Nil,
        ParentConcept(ConceptReference("parentConcept1"),None, Map(), Nil) :: Nil,
        AnonymousFunctionDefinition(
          Nil,
          ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
        ),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept by () => read_csv_file(\"in.csv\")"
    ).get should equal(
      FunctionConcept(
        "myConcept",
        Nil,
        Nil,
        AnonymousFunctionDefinition(
          Nil,
          ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
        ),
        Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "concept myConcept by read_myconcept_from_csv_file)"
    ).get should equal(
      FunctionConcept(
        "myConcept",
        Nil,
        Nil,
        FunctionReference("read_myconcept_from_csv_file"),
        Nil
      )
    )
  }

  "Union concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")\n" +
      "concept myConcept union of\n" +
      "parentConcept1," +
      "< (attr1, attr2) from parentConcept2 >,\n" +
      "< read_csv_file(\"in.csv\") >"
    ).get should equal(
      UnionConcept(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
          ParentConcept(ConceptReference("parentConcept2"), None, Map(), Nil) :: Nil)
          .build,
          None, Map(), Nil) ::
        ParentConcept(AnonymousFunctionConceptDefinition(
          ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
        ), None, Map(), Nil) :: Nil,
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Intersect concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")\n" +
        "concept myConcept intersect of\n" +
        "parentConcept1," +
        "< (attr1, attr2) from parentConcept2 >,\n" +
        "< read_csv_file(\"in.csv\") >"
    ).get should equal(
      IntersectConcept(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), None, Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
            ParentConcept(ConceptReference("parentConcept2"), None, Map(), Nil) :: Nil)
            .build,
            None, Map(), Nil) ::
          ParentConcept(AnonymousFunctionConceptDefinition(
            ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
          ), None, Map(), Nil) :: Nil,
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Minus concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")\n" +
        "concept myConcept minus\n" +
        "parentConcept1," +
        "< (attr1, attr2) from parentConcept2 >,\n" +
        "< read_csv_file(\"in.csv\") >"
    ).get should equal(
      MinusConcept(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), None, Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
            ParentConcept(ConceptReference("parentConcept2"), None, Map(), Nil) :: Nil)
            .build,
            None, Map(), Nil) ::
          ParentConcept(AnonymousFunctionConceptDefinition(
            ProcedureCall(expression.FunctionCall(FunctionReference("read_csv_file"), StringLiteral("in.csv") :: Nil))
          ), None, Map(), Nil) :: Nil,
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )
  }

  "Nested concept definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
        "concept myConcept is\n" +
        "parentConcept1 c1\n" +
        "where not exists <parentConcept2 c2 where c1.id = c2.c1Ref>"
    ).get should equal(
      InheritedConcept.builder(
        "myConcept",
        ParentConcept(ConceptReference("parentConcept1"), Some("c1"), Map(), Nil) :: Nil)
        .additionalDependencies(Not(Exists(
          AnonymousConceptDefinition.builder(
            Nil,
            ParentConcept(ConceptReference("parentConcept2"), Some("c2"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(
              ConceptAttribute("c1" :: Nil, "id"),
              ConceptAttribute("c2" :: Nil, "c1Ref")
            ))
            .build
        )))
        .build
    )
  }

  "File data source" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.dataSourceParser,
        "CSV file \"myfile.csv\""
    ).get should equal(
      FileDataSource("myfile.csv", FileFormats.CSV)
    )
  }

  "DataSource definition" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensConcept,
      "@Owner(name = \"Al1\")\n" +
        "datasource myDataSource (@NotNull attr1, attr2)\n" +
        "from CSV file \"myfile.csv\""
    ).get should equal(
      DataSourceConcept(
        "myDataSource",
        Attribute("attr1", None, Annotation("NotNull", Map()) :: Nil) ::
          Attribute("attr2", None, Nil) :: Nil,
        FileDataSource("myfile.csv", FileFormats.CSV),
        Annotation("Owner", Map("name" -> StringLiteral("Al1"))) :: Nil
      )
    )

    conceptParser.parse(
      conceptParser.sensConcept,
      "@Source(path = \"my_bucket\")\n" +
        "datasource myDataSource (" +
        "@NotNull string(36) id, " +
        "int categoryId, " +
        "float amount)\n" +
        "from CSV file \"myfile.csv\""
    ).get should equal(
      DataSourceConcept(
        "myDataSource",
        Attribute("id", None, Annotation("NotNull", Map()) :: Nil, Some(StringTypeLiteral(36))) ::
          Attribute("categoryId", None, Nil, Some(IntTypeLiteral())) ::
          Attribute("amount", None, Nil, Some(FloatTypeLiteral())) :: Nil,
        FileDataSource("myfile.csv", FileFormats.CSV),
        Annotation("Source", Map("path" -> StringLiteral("my_bucket"))) :: Nil
      )
    )
  }

  "Concept definition statement" should "be parsed correctly" in {
    conceptParser.parse(
      conceptParser.sensStatement,
      "{\n" +
      "concept myConcept1 (attr1 = c.attr1, attr2 = c.attr2) from parentConcept c;\n" +
      "concept myConcept2 is parentConcept with type = \"someType\" without attr1, attr2" +
      "}"
    ).get should equal (
      StatementBlock(
        ConceptDefinition(
          Concept.builder(
            "myConcept1",
            Attribute("attr1", Some(ConceptAttribute("c" :: Nil, "attr1")), List()) ::
              Attribute("attr2", Some(ConceptAttribute("c" :: Nil, "attr2")), List()) :: Nil,
            ParentConcept(ConceptReference("parentConcept"), Some("c"), Map(), Nil) :: Nil)
            .build
        ) ::
        ConceptDefinition(
          InheritedConcept.builder(
            "myConcept2",
            ParentConcept(ConceptReference("parentConcept"), None, Map(), Nil) :: Nil)
            .overriddenAttributes(Attribute("type", Some(StringLiteral("someType")), Nil) :: Nil)
            .removedAttributes(ConceptAttribute(Nil, "attr1") :: ConceptAttribute(Nil, "attr2") :: Nil)
            .build
        ) :: Nil
      )
    )
  }

  "Incorrect Program" should "return unrecognizable tokens" in {
    conceptParser.parseDataModel(
      "concept myConcept1 (attr1 = c.attr1, attr2 = c.attr2) from parentConcept c;\n" +
        "some unrecognizable text;" +
        "concept myConcept2 is parentConcept with type = \"someType\" without attr1, attr2;"
    ).isLeft should be (true)


  }

}
