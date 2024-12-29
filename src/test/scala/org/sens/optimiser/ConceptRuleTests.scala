package org.sens.optimiser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.optimization.concept.{ConvertExistsToParentConceptRule, ConvertInToParentConceptRule, RemoveUnusedAttributesRule}
import org.sens.core.concept.{Annotation, Attribute, Concept, DataSourceConcept, ParentConcept, UnionConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.FunctionCall
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.expression.operation.relational.{Exists, InSubQuery}
import org.sens.core.expression.operation.logical.{Not, Or}
import org.sens.core.expression.literal.{BooleanLiteral, FloatLiteral, NullLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.{Equals, NotEquals}
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class ConceptRuleTests extends AnyFlatSpec with Matchers {

  "Unused concept attributes" should "be removed from parent concept" in {
    val context = ValidationContext()
    val childConcept = Concept.builder("avgPrice",
      Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
        Attribute("avgPrice", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca")))
      .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    val parentConcept = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept))

    RemoveUnusedAttributesRule(parentConcept, childConcept :: Nil, context) should equal (
      DataSourceConcept(
        "product",
        Attribute("category", None, Nil) ::
          Attribute("country", None, Nil) ::
          Attribute("price", None, Nil) :: Nil,
        FileDataSource("someFile", FileFormats.CSV),
        Nil
      )
    )
  }

  "Chain of unused concept attributes" should "be replaced in parent concept" in {
    val context = ValidationContext()
    val childConcept = Concept.builder("avgPrice",
      Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
        Attribute("avgPrice", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("productView"), Some("p"), Map(), Nil) :: Nil)
      .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    val rootConcept = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(rootConcept))

    val parentConcept = Concept.builder(
      "productView",
      Attribute("id", Some(ConceptAttribute("p" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("p" :: Nil, "name")), Nil) ::
        Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
        Attribute("country", Some(ConceptAttribute("p" :: Nil, "country")), Nil) ::
        Attribute("description", Some(ConceptAttribute("p" :: Nil, "description")), Nil) ::
        Attribute("price", Some(Add(ConceptAttribute(Nil, "basePrice"), ConceptAttribute(Nil, "tax"))), Nil) ::
        Attribute("tax", Some(Multiply(ConceptAttribute(Nil, "basePrice"), FloatLiteral(0.13))), Nil) ::
        Attribute("basePrice", Some(ConceptAttribute("p" :: Nil, "price")), Nil) :: Nil,
      ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
      .build()
    context.addConcept(ConceptDefinition(parentConcept))

    RemoveUnusedAttributesRule(parentConcept, childConcept :: Nil, context) should equal(
      Concept.builder(
        "productView",
          Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
          Attribute("price", Some(
            Add(
              ConceptAttribute("p" :: Nil, "price"),
              Multiply(
                ConceptAttribute("p" :: Nil, "price"),
                FloatLiteral(0.13)
              )
            )
          ), Nil) :: Nil,
        ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca")))
        .build()
    )
  }

  "Unused concept attributes" should "be removed from all parent concept" in {
    val context = ValidationContext()
    val parentConcept = DataSourceConcept(
      "stats",
      Attribute("date", None, Nil) ::
        Attribute("metric1", None, Nil) ::
        Attribute("metric2", None, Nil) ::
        Attribute("metric3", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept))

    val childConcept1 = Concept.builder("monthAvgStats",
      Attribute("month", Some(FunctionCall(FunctionReference("month"), ConceptAttribute("s" :: Nil, "date") :: Nil)), Nil) ::
        Attribute("avgMetric1", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("s" :: Nil, "metric1") :: Nil)), Nil) ::
        Nil,
      ParentConcept(ConceptReference("stats"), Some("s"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute(Nil, "month"), FunctionCall(FunctionReference("cur_month"), Nil)))
      .groupByAttributes(ConceptAttribute(Nil, "month") :: Nil)
      .build()
    context.addConcept(ConceptDefinition(childConcept1))

    val childConcept2 = Concept.builder("todayStats",
      Attribute("date", Some(ConceptAttribute("s" :: Nil, "date")), Nil) ::
        Attribute("metric1", Some(ConceptAttribute("s" :: Nil, "metric1")), Nil) ::
        Attribute("metric2", Some(ConceptAttribute("s" :: Nil, "metric2")), Nil) ::
        Nil,
      ParentConcept(ConceptReference("stats"), Some("s"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute(Nil, "date"), FunctionCall(FunctionReference("cur_date"), Nil)))
      .build()
    context.addConcept(ConceptDefinition(childConcept2))

    RemoveUnusedAttributesRule(parentConcept, childConcept1 :: childConcept2 :: Nil, context) should equal(
      DataSourceConcept(
        "stats",
        Attribute("date", None, Nil) ::
          Attribute("metric1", None, Nil) ::
          Attribute("metric2", None, Nil) :: Nil,
        FileDataSource("someFile", FileFormats.CSV),
        Nil
      )
    )

  }

  "Unused attributes of union concept" should "be removed from parent concept" in {
    val context = ValidationContext()
    val rootConcept1 = DataSourceConcept(
      "statsV1",
      Attribute("date", None, Nil) ::
        Attribute("metric1", None, Nil) ::
        Attribute("metric2", None, Nil) ::
        Attribute("metric3", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(rootConcept1))

    val rootConcept2 = DataSourceConcept(
      "statsV2",
      Attribute("date", None, Nil) ::
        Attribute("impressions", None, Nil) ::
        Attribute("clicks", None, Nil) ::
        Attribute("registrations", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(rootConcept2))

    val parentConcept = UnionConcept(
      "statsAll",
      ParentConcept(ConceptReference("statsV2"), None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("date", Some(ConceptAttribute("s" :: Nil, "date")), Nil) ::
            Attribute("impressions", Some(ConceptAttribute("s" :: Nil, "metric1")), Nil) ::
            Attribute("clicks", Some(ConceptAttribute("s" :: Nil, "metric2")), Nil) ::
            Attribute("registrations", Some(ConceptAttribute("s" :: Nil, "metric3")), Nil) :: Nil,
          ParentConcept(ConceptReference("statsV1"), Some("s"), Map(), Nil) :: Nil)
          .build(),
          None, Map(), Nil) :: Nil,
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept))

    val childConcept = Concept.builder("todayStats",
      Attribute("date", Some(ConceptAttribute("s" :: Nil, "date")), Nil) ::
        Attribute("impressions", Some(ConceptAttribute("s" :: Nil, "impressions")), Nil) ::
        Attribute("clicks", Some(ConceptAttribute("s" :: Nil, "clicks")), Nil) ::
        Nil,
      ParentConcept(ConceptReference("statsAll"), Some("s"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute(Nil, "date"), FunctionCall(FunctionReference("cur_date"), Nil)))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    RemoveUnusedAttributesRule(parentConcept, childConcept :: Nil, context) should equal(
      UnionConcept(
        "statsAll",
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("date", Some(ConceptAttribute("statsV2" :: Nil, "date")), Nil) ::
              Attribute("impressions", Some(ConceptAttribute("statsV2" :: Nil, "impressions")), Nil) ::
              Attribute("clicks", Some(ConceptAttribute("statsV2" :: Nil, "clicks")), Nil) :: Nil,
            ParentConcept(ConceptReference("statsV2"), None, Map(), Nil) :: Nil).build(),
          None, Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("date", Some(ConceptAttribute("s" :: Nil, "date")), Nil) ::
              Attribute("impressions", Some(ConceptAttribute("s" :: Nil, "metric1")), Nil) ::
              Attribute("clicks", Some(ConceptAttribute("s" :: Nil, "metric2")), Nil) :: Nil,
            ParentConcept(ConceptReference("statsV1"), Some("s"), Map(), Nil) :: Nil)
            .build(),
            None, Map(), Nil) :: Nil,
        Nil
      )
    )
  }

  "Exists condition in top And must be " should "replaced with parent concept and true condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(Exists(
        AnonymousConceptDefinition.builder(
          Nil,
          ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
        )
          .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
          .build()
      ))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertExistsToParentConceptRule(childConcept, context) should equal (
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("exists_attribute_1", Some(BooleanLiteral(true)), Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
            .build(), Some("acd_exists_1"), Map(), Nil) :: Nil
      )
        .attributeDependencies(BooleanLiteral(true))
        .annotations(Annotation.UNIQUE :: Nil)
        .build()
    )
  }

  "Exists condition in Or must be " should "replaced with optional parent concept and is not null condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(Or(
        Equals(ConceptAttribute("c" :: Nil, "parentCategory"), NullLiteral()),
        Exists(AnonymousConceptDefinition.builder(
          Nil,
          ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
        )
          .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
          .build()
      )))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertExistsToParentConceptRule(childConcept, context) should equal(
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("exists_attribute_1", Some(BooleanLiteral(true)), Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
            .build(), Some("acd_exists_1"), Map(), Annotation.OPTIONAL :: Nil) :: Nil
      )
        .attributeDependencies(Or(
          Equals(ConceptAttribute("c" :: Nil, "parentCategory"), NullLiteral()),
          NotEquals(ConceptAttribute("acd_exists_1" :: Nil, "exists_attribute_1"), NullLiteral())
        ))
        .annotations(Annotation.UNIQUE :: Nil)
        .build()
    )
  }

  "Not Exists condition must be " should "replaced with optional parent concept and is null condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(Not(Exists(
        AnonymousConceptDefinition.builder(
          Nil,
          ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
        )
          .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
          .build()
      )))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertExistsToParentConceptRule(childConcept, context) should equal(
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("exists_attribute_1", Some(BooleanLiteral(true)), Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .attributeDependencies(Equals(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("c" :: Nil, "id")))
            .build(), Some("acd_exists_1"), Map(), Annotation.OPTIONAL :: Nil) :: Nil
      )
        .attributeDependencies(Equals(ConceptAttribute("acd_exists_1" :: Nil, "exists_attribute_1"), NullLiteral()))
        .build()
    )
  }

  "In condition in top And must be " should "replaced with parent concept and true condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(InSubQuery(
        ConceptAttribute("c" :: Nil, "id"),
        AnonymousConceptDefinition.builder(
          Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
          ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
        )
          .build()
      ))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertInToParentConceptRule(childConcept, context) should equal(
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .build(),
            Some("acd_exists_1"),
            Map("category" -> ConceptAttribute("c" :: Nil, "id")),
            Nil) :: Nil
      )
        .attributeDependencies(BooleanLiteral(true))
        .annotations(Annotation.UNIQUE :: Nil)
        .build()
    )
  }

  "In condition in Or must be " should "replaced with optional parent concept and is not null condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(Or(
        Equals(ConceptAttribute("c" :: Nil, "parentCategory"), NullLiteral()),
        InSubQuery(
          ConceptAttribute("c" :: Nil, "id"),
          AnonymousConceptDefinition.builder(
            Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .build()
      )))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertInToParentConceptRule(childConcept, context) should equal(
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .build(),
            Some("acd_exists_1"),
            Map("category" -> ConceptAttribute("c" :: Nil, "id")),
            Annotation.OPTIONAL :: Nil) :: Nil
      )
        .attributeDependencies(Or(
          Equals(ConceptAttribute("c" :: Nil, "parentCategory"), NullLiteral()),
          NotEquals(ConceptAttribute("acd_exists_1" :: Nil, "category"), NullLiteral())
        ))
        .annotations(Annotation.UNIQUE :: Nil)
        .build()
    )
  }

  "Not In condition must be " should "replaced with optional parent concept and is null condition" in {
    val context = ValidationContext()

    val parentConcept1 = DataSourceConcept(
      "product",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("description", None, Nil) ::
        Attribute("price", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept1))

    val parentConcept2 = DataSourceConcept(
      "category",
      Attribute("id", None, Nil) ::
        Attribute("name", None, Nil) ::
        Attribute("parentCategory", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )
    context.addConcept(ConceptDefinition(parentConcept2))

    val childConcept = Concept.builder(
      "emptyCategory",
      Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
        Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
      ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) :: Nil
    )
      .attributeDependencies(Not(InSubQuery(
        ConceptAttribute("c" :: Nil, "id"),
        AnonymousConceptDefinition.builder(
          Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
          ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
        )
          .build()
      )))
      .build()
    context.addConcept(ConceptDefinition(childConcept))

    ConvertInToParentConceptRule(childConcept, context) should equal(
      Concept.builder(
        "emptyCategory",
        Attribute("id", Some(ConceptAttribute("c" :: Nil, "id")), Nil) ::
          Attribute("name", Some(ConceptAttribute("c" :: Nil, "name")), Nil) :: Nil,
        ParentConcept(ConceptReference("category"), Some("c"), Map(), Nil) ::
          ParentConcept(AnonymousConceptDefinition.builder(
            Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Annotation.UNIQUE :: Nil) :: Nil,
            ParentConcept(ConceptReference("product"), Some("p"), Map(), Nil) :: Nil
          )
            .build(),
            Some("acd_exists_1"),
            Map("category" -> ConceptAttribute("c" :: Nil, "id")),
            Annotation.OPTIONAL :: Nil) :: Nil
      )
        .attributeDependencies(Equals(ConceptAttribute("acd_exists_1" :: Nil, "category"), NullLiteral()))
        .build()
    )
  }

}
