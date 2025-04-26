package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, Concept, DataSourceConcept, FunctionConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.function.AnonymousFunctionDefinition
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.statement.{ConceptDefinition, NOP}
import org.sens.parser.ValidationContext

import scala.util.Success

class ConceptInferenceTests extends AnyFlatSpec with Matchers {

  "Concept definition" should "infer attributes expressions correctly" in {
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

    val conDef1 = Concept.builder(
      "conceptA",
      Attribute("attrA", None, Nil) ::
      Attribute("attrB", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(
        And(
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
          And(
            Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1")),
            Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cb" :: Nil, "attr2"))
          )))
      .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
      .build

    val inferredConcept1 = conDef1.inferAttributeExpressions(context)
    inferredConcept1 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))

    val conDef2 = Concept.builder(
      "conceptA",
      Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr3" -> StringLiteral("SomeValue")), Nil) :: Nil)
      .attributeDependencies(
        And(
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
          Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cb" :: Nil, "attr2"))
        ))
      .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
      .build

    val inferredConcept2 = conDef2.inferAttributeExpressions(context)
    inferredConcept2 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        ))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))

    val conDef3 = Concept.builder(
      "conceptA",
      Attribute("attrA", None, Nil) ::
        Attribute("attrB", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr3" -> StringLiteral("SomeValue"), "attr1" -> ConceptAttribute(Nil, "attrA")), Nil) :: Nil)
      .attributeDependencies(
        And(
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
          Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cb" :: Nil, "attr2"))
        ))
      .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
      .build

    val inferredConcept3 = conDef3.inferAttributeExpressions(context)
    inferredConcept3 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        ))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))
  }

  "Concept definition" should "infer attributes expressions correctly for joins" in {
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

    val conDef1 = Concept.builder(
      "conceptA",
      Attribute("attrA", None, Nil) ::
      Attribute("attrB", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) :: Nil)
      .attributeDependencies(
        And(Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
        And(Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1")),
        And(Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cc" :: Nil, "attr2")),
        And(Equals(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1")),
          Equals(ConceptAttribute(Nil, "attrA"), IntLiteral(1))
        )))))
      .build

    val inferredConcept1 = conDef1.inferAttributeExpressions(context)
    inferredConcept1 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute(List(), "attrA"), IntLiteral(1))))
        .build
    ))

    val conDef2 = Concept.builder(
      "conceptA",
      Attribute("attrA", Some(IntLiteral(1)), Nil) ::
      Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) :: Nil)
      .attributeDependencies(
        And(Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
            Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1"))
        ))
      .build

    val inferredConcept2 = conDef2.inferAttributeExpressions(context)
    inferredConcept2 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(IntLiteral(1)), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1"))
        ))
        .build
    ))

    val conDef3 = Concept.builder(
      "conceptA",
      Attribute("attrA", Some(IntLiteral(1)), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cd" :: Nil, "attr1")), Nil) ::
        ParentConcept(ConceptReference("conceptD"), Some("cd"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
        Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1"))
      ))
      .build

    val inferredConcept3 = conDef3.inferAttributeExpressions(context)
    inferredConcept3 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(IntLiteral(1)), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          ParentConcept(ConceptReference("conceptD"), Some("cd"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1"))
        )).build
    ))
  }

  "Concept definition" should "infer attributes expressions correctly from parent concept if expressions are not defined" in {
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

    val conDef1 = Concept.builder(
      "conceptA",
      Attribute("attrA", None, Nil) ::
      Attribute("attrB", None, Nil) ::
      Attribute("attr4", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(
        And(
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
          And(
            Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1")),
            Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cb" :: Nil, "attr2"))
          )))
      .build

    val inferredConcept1 = conDef1.inferAttributeExpressions(context)
    inferredConcept1 should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")))
        .build
    ))
  }

  "Attribute inference of concept definition" should "fail if attributes are not defined" in {
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

      val conDef1 = Concept.builder(
        "conceptA",
        Attribute("attr1", None, Nil) ::
          Attribute("attrB", Some(Add(ConceptAttribute(Nil, "attr1"), IntLiteral(1))), Nil) ::
          Attribute("attrC", Some(Multiply(ConceptAttribute(Nil, "attrB"), IntLiteral(2))), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .build()

      val inferredConcept1 = conDef1.inferAttributeExpressions(context)
      inferredConcept1 should equal(Success(
        Concept.builder(
          "conceptA",
          Attribute("attr1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
            Attribute("attrB", Some(Add(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(1))), Nil) ::
            Attribute("attrC", Some(Multiply(Add(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(1)), IntLiteral(2))), Nil) :: Nil,
          ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
          .build()
      ))
    }

  "Attribute inference of concept definition" should "remove links to child attributes" in {

  }

  "Nested Anonymous Concept definition" should "infer attributes expressions correctly" in {
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
      Attribute("attrA", None, Nil) ::
        Attribute("attrB", None, Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          AnonymousConceptDefinition.builder(
            Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
            ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
              ParentConcept(ConceptReference("conceptD"), Some("cd"), Map(), Nil) ::
              Nil)
            .attributeDependencies(And(
              Equals(ConceptAttribute(Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1")),
              And(
                Equals(ConceptAttribute(Nil, "attr2"), ConceptAttribute("cc" :: Nil, "attr2")),
                Equals(ConceptAttribute("cd" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1"))
              )
            ))
            .build,
          Some("cc"),
          Map(),
          Nil) :: Nil)
      .attributeDependencies(
        And(Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("SomeValue")),
          And(Equals(ConceptAttribute(Nil, "attrA"), ConceptAttribute("cb" :: Nil, "attr1")),
            And(Equals(ConceptAttribute(Nil, "attrB"), ConceptAttribute("cc" :: Nil, "attr2")),
              And(Equals(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1")),
                Equals(ConceptAttribute(Nil, "attrA"), IntLiteral(1))
              )))))
      .build

    val inferredConcept = conDef.inferAttributeExpressions(context)
    inferredConcept should equal(Success(
      Concept.builder(
        "conceptA",
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
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
          Equals(ConceptAttribute(List(), "attrA"), IntLiteral(1))))
        .build
    ))
  }

  "Reference to Function Concept" should "infer attributes expressions correctly" in {
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
        Attribute("cat1", None, Nil) ::
        Attribute("cat2", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr2" -> IntLiteral(1)), Nil) :: Nil)
      .attributeDependencies(And(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
        And(
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), ConceptAttribute(Nil, "cat1")),
          Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("someVal"))
        )
      ))
      .build
    val inferredConcept1 = conDef1.inferAttributeExpressions(context)
    inferredConcept1 should equal(Success(
      Concept.builder("conceptA",
        Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("cat1", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
          Attribute("cat2", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
          Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr2" -> IntLiteral(1), "attr3" -> StringLiteral("someVal")), Nil) :: Nil)
        .attributeDependencies(
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        )
        .build
    ))

    val conDef2 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
        And(
          And(
            Equals(ConceptAttribute("cb" :: Nil, "attr2"), IntLiteral(1)),
            Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("someVal"))
          ),
          Equals(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1"))
        )
      ))
      .build

    val inferredConcept2 = conDef2.inferAttributeExpressions(context)
    inferredConcept2 should equal(Success(
      Concept.builder("conceptA",
        Attribute("id", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) ::
          ParentConcept(
            ConceptReference("conceptB"),
            Some("cb"),
            Map(
              "attr1" -> ConceptAttribute("cc" :: Nil, "attr1"),
              "attr2" -> IntLiteral(1),
              "attr3" -> StringLiteral("someVal")),
            Nil) :: Nil)
        .attributeDependencies(
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        )
        .build
    ))

    val conDef3 = Concept.builder("conceptA",
      Attribute("id", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0)),
        And(
          And(
            Equals(ConceptAttribute("cb" :: Nil, "attr2"), IntLiteral(1)),
            Equals(ConceptAttribute("cb" :: Nil, "attr3"), StringLiteral("someVal"))
          ),
          Equals(ConceptAttribute("cb" :: Nil, "attr1"), ConceptAttribute("cc" :: Nil, "attr1"))
        )
      ))
      .build

    val inferredConcept3 = conDef3.inferAttributeExpressions(context)
    inferredConcept3 should equal(Success(
      Concept.builder("conceptA",
        Attribute("id", Some(ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(
          ConceptReference("conceptB"),
          Some("cb"),
          Map(
            "attr2" -> IntLiteral(1),
            "attr3" -> StringLiteral("someVal")),
          Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr4"), IntLiteral(0))
        )
        .build
    ))
  }
}
