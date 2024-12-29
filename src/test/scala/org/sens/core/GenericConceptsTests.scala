package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.parser.ValidationContext
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.statement.ConceptDefinition
import org.sens.core.concept._
import org.sens.core.expression.{ConceptAttribute, FunctionCall, GenericParameter}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.comparison.Equals

class GenericConceptsTests extends AnyFlatSpec with Matchers {

  "Idents" should "be formatted in Sens correctly" in {
    ConstantIdent("name").toSensString should equal ("name")
    ConstantIdent("name").getName should equal ("name")

    ExpressionIdent(Add(GenericParameter("name"), FunctionCall(FunctionReference("current_date"), Nil))).toSensString should equal ("$(name + current_date())")
    ExpressionIdent(StringLiteral("name")).getName should equal ("name")
  }

  "GenericConceptReference" should "be formatted in Sens correctly" in {
    GenericConceptReference(ConstantIdent("name"), Map("param" -> StringLiteral("value"))).toSensString should equal (
      "name[param: \"value\"]"
    )
    GenericConceptReference(ExpressionIdent(GenericParameter("name")), Map()).toSensString should equal(
      "$name"
    )
  }

  "Generic Concept" should "be formatted in Sens correctly" in {
    val conDef = Concept.builder(
      "myConcept",
      Attribute("key", Some(ConceptAttribute("g" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("g" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map("conceptName" -> GenericParameter("sourceName"))),
        Some("g"), Map(), Nil) :: Nil
    ).genericParameters("conceptName" :: "sourceName" :: Nil).build()
    conDef.toSensString should equal (
      "concept myConcept[conceptName, sourceName] (key = g.attr1,\n" +
        "val = g.attr2)\n" +
        "from $conceptName[conceptName: sourceName] g"
    )
  }

  "Generic Concept" should "be instantiated correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG1",
        Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) :: Nil,
        ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map()), Some("p"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: Nil).build()
    ))

    val conDef = Concept.builder(
      "conceptA",
      Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ConstantIdent("conceptG1"), Map("conceptName" -> StringLiteral("conceptT1"))),
        Some("g"),
        Map(),
        Nil
      ) :: Nil
    ).build()

    conDef.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          ConceptReference("_generic_conceptG1_conceptT1"),
          Some("g"),
          Map(),
          Nil
        ) :: Nil
      ).build()
    )

    context.containsConcept("_generic_conceptG1_conceptT1") should be (true)
  }

  "Generic Concept" should "instantiate its own generic concepts correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT3",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG1",
        Attribute("key", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) ::Nil,
        ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map()), Some("p"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: Nil).build()
    ))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG2",
        Attribute("key", Some(ConceptAttribute("g" :: Nil, "key")), Nil) ::
          Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map("conceptName" -> GenericParameter("sourceName"))),
          Some("g"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: "sourceName" :: Nil).build()
    ))

    val conDef = Concept.builder(
      "conceptA",
      Attribute("key", Some(ConceptAttribute("g1" :: Nil, "key")), Nil) ::
        Attribute("val1", Some(ConceptAttribute("g1" :: Nil, "val")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("g2" :: Nil, "val")), Nil) ::Nil,
      ParentConcept(
        GenericConceptReference(ConstantIdent("conceptG1"), Map("conceptName" -> StringLiteral("conceptT1"))),
        Some("g1"), Map(),Nil
      ) ::
        ParentConcept(
          GenericConceptReference(
            ConstantIdent("conceptG2"),
            Map("conceptName" -> StringLiteral("conceptG1"), "sourceName" -> StringLiteral("conceptT3"))),
          Some("g2"), Map(), Nil
        ) :: Nil
    ).attributeDependencies(Equals(ConceptAttribute("g1" :: Nil, "key"), ConceptAttribute("g2" :: Nil, "key")))
      .build()

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "conceptA",
        Attribute("key", Some(ConceptAttribute("g1" :: Nil, "key")), Nil) ::
          Attribute("val1", Some(ConceptAttribute("g1" :: Nil, "val")), Nil) ::
          Attribute("val2", Some(ConceptAttribute("g2" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          ConceptReference("_generic_conceptG1_conceptT1"),
          Some("g1"), Map(), Nil
        ) ::
          ParentConcept(
            ConceptReference("_generic_conceptG2_conceptG1_conceptT3"),
            Some("g2"), Map("key" -> ConceptAttribute("g1" :: Nil, "key")), Nil
          ) :: Nil
      ).build()
    )

    context.containsConcept("_generic_conceptG1_conceptT1") should be (true)
    context.containsConcept("_generic_conceptG2_conceptG1_conceptT3") should be (true)
    context.containsConcept("_generic_conceptG1_conceptT3") should be (true)
  }

  "Anonymous Generic Concept" should "be instantiated correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG1",
        Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) :: Nil,
        ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map()), Some("p"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: Nil).build()
    ))

    val conDef = Concept.builder(
      "conceptA",
      Attribute("val", Some(ConceptAttribute("a" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(
        AnonymousConceptDefinition.builder(
          Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
          ParentConcept(
            GenericConceptReference(ConstantIdent("conceptG1"), Map("conceptName" -> StringLiteral("conceptT1"))),
            Some("g"),
            Map(),
            Nil
          ) :: Nil
        ).build(),
        Some("a"),
        Map(),
        Nil
      ) :: Nil
    ).build()

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "conceptA",
        Attribute("val", Some(ConceptAttribute("a" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
            ParentConcept(
              ConceptReference("_generic_conceptG1_conceptT1"),
              Some("g"),
              Map(),
              Nil
            ) :: Nil
          ).build(),
          Some("a"),
          Map(),
          Nil
        ) :: Nil
      ).build()
    )

    context.containsConcept("_generic_conceptG1_conceptT1") should be(true)
  }

  "Anonimous Generic Concept" should "instantiate its own generic concepts correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT3",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG1",
        Attribute("key", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map()), Some("p"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: Nil).build()
    ))
    context.addConcept(ConceptDefinition(
      Concept.builder(
        "conceptG2",
        Attribute("key", Some(ConceptAttribute("a" :: Nil, "key")), Nil) ::
          Attribute("val", Some(ConceptAttribute("a" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("key", Some(ConceptAttribute("g" :: Nil, "key")), Nil) ::
              Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
            ParentConcept(
              GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map("conceptName" -> GenericParameter("sourceName"))),
              Some("g"),
              Map(),
              Nil
            ) :: Nil
          ).build(),
          Some("a"), Map(), Nil) :: Nil
      ).genericParameters("conceptName" :: "sourceName" :: Nil).build()
    ))

    val conDef = Concept.builder(
      "conceptA",
      Attribute("key", Some(ConceptAttribute("g1" :: Nil, "key")), Nil) ::
        Attribute("val1", Some(ConceptAttribute("g1" :: Nil, "val")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("g2" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(
        GenericConceptReference(ConstantIdent("conceptG1"), Map("conceptName" -> StringLiteral("conceptT1"))),
        Some("g1"), Map(), Nil
      ) ::
        ParentConcept(
          GenericConceptReference(
            ConstantIdent("conceptG2"),
            Map("conceptName" -> StringLiteral("conceptG1"), "sourceName" -> StringLiteral("conceptT3"))),
          Some("g2"), Map(), Nil
        ) :: Nil
    ).attributeDependencies(Equals(ConceptAttribute("g1" :: Nil, "key"), ConceptAttribute("g2" :: Nil, "key")))
      .build()

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "conceptA",
        Attribute("key", Some(ConceptAttribute("g1" :: Nil, "key")), Nil) ::
          Attribute("val1", Some(ConceptAttribute("g1" :: Nil, "val")), Nil) ::
          Attribute("val2", Some(ConceptAttribute("g2" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          ConceptReference("_generic_conceptG1_conceptT1"),
          Some("g1"), Map(), Nil
        ) ::
          ParentConcept(
            ConceptReference("_generic_conceptG2_conceptG1_conceptT3"),
            Some("g2"), Map("key" -> ConceptAttribute("g1" :: Nil, "key")), Nil
          ) :: Nil
      ).build()
    )

    context.containsConcept("_generic_conceptG1_conceptT1") should be(true)
    context.containsConcept("_generic_conceptG2_conceptG1_conceptT3") should be(true)
    context.containsConcept("_generic_conceptG1_conceptT3") should be(true)
    context.getConcept("_generic_conceptG2_conceptG1_conceptT3").get.concept should equal (
      Concept.builder(
        "_generic_conceptG2_conceptG1_conceptT3",
        Attribute("key", Some(ConceptAttribute("a" :: Nil, "key")), Nil) ::
          Attribute("val", Some(ConceptAttribute("a" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("key", Some(ConceptAttribute("g" :: Nil, "key")), Nil) ::
              Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
            ParentConcept(
              ConceptReference("_generic_conceptG1_conceptT3"),
              Some("g"),
              Map(),
              Nil
            ) :: Nil
          ).build(),
          Some("a"), Map(), Nil) :: Nil
      ).build()
    )
  }

  "Generic Inherited Concept" should "be formatted in Sens correctly" in {
    val conDef = InheritedConcept.builder(
      "myConcept",
      ParentConcept(
        GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map("conceptName" -> GenericParameter("sourceName"))),
        Some("g"), Map(), Nil) :: Nil
    ).overriddenAttributes(
      Attribute("key", Some(ConceptAttribute("g" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("g" :: Nil, "attr2")), Nil) :: Nil
    ).genericParameters("conceptName" :: "sourceName" :: Nil).build()
    conDef.toSensString should equal(
      "concept myConcept[conceptName, sourceName] is $conceptName[conceptName: sourceName] g\n" +
        "with key = g.attr1, val = g.attr2"
    )
  }

  "Generic Inherited Concept" should "be instantiated correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptT1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(
      InheritedConcept.builder(
        "conceptG1",
        ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("conceptName")), Map()), Some("p"), Map(), Nil) :: Nil
      ).overriddenAttributes(Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) :: Nil)
        .genericParameters("conceptName" :: Nil).build()
    ))

    val conDef = InheritedConcept.builder(
      "conceptA",
      ParentConcept(
        GenericConceptReference(ConstantIdent("conceptG1"), Map("conceptName" -> StringLiteral("conceptT1"))),
        Some("g"),
        Map(),
        Nil
      ) :: Nil
    ).removedAttributes(ConceptAttribute("g" :: Nil, "attr1") :: ConceptAttribute("g" :: Nil, "attr2") :: Nil)
      .build()

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "conceptA",
        Attribute("val", Some(ConceptAttribute("g" :: Nil, "val")), Nil) :: Nil,
        ParentConcept(
          ConceptReference("_generic_conceptG1_conceptT1"),
          Some("g"),
          Map(),
          Nil
        ) :: Nil
      ).build()
    )

    context.containsConcept("_generic_conceptG1_conceptT1") should be(true)
    context.getConcept("_generic_conceptG1_conceptT1").get.concept should equal (
      InheritedConcept.builder(
        "_generic_conceptG1_conceptT1",
        ParentConcept(ConceptReference("conceptT1"), Some("p"), Map(), Nil) :: Nil
      ).overriddenAttributes(Attribute("val", Some(ConceptAttribute("p" :: Nil, "attr1")), Nil) :: Nil)
        .build()
    )
  }
}
