package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Attribute, DataSourceConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.{AttributeExpressionNotFound, ValidationContext}

import scala.util.{Failure, Success}

class AnonymousConceptDefinitionInferenceTests extends AnyFlatSpec with Matchers {

  "Anonymous Concept definition" should "infer attributes expressions correctly" in {
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

    val conDef1 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))

    val conDef2 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          GreaterThan(ConceptAttribute(List("cb"), "attr4"), IntLiteral(0))
        ))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))

    val conDef3 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          GreaterThan(ConceptAttribute(List("cb"), "attr4"), IntLiteral(0))
        ))
        .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
        .build
    ))
  }

  "Anonymous Concept definition" should "infer attributes expressions correctly for joins" in {
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

    val conDef1 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute(List(), "attrA"), IntLiteral(1))
        ))
      .build
    ))

    val conDef2 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(IntLiteral(1)), Nil) ::
          Attribute("attrB", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
          ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute(List(), "attrA"), ConceptAttribute(List("cb"), "attr1"))))
        .build()
    ))
  }

  "Anonymous Concept definition" should "infer attributes expressions correctly from parent concept if expressions are not defined" in {
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

    val conDef1 = AnonymousConceptDefinition.builder(
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
      AnonymousConceptDefinition.builder(
        Attribute("attrA", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attrB", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr3"), StringLiteral("SomeValue")))
        .build
    ))
  }

  "Attribute inference of anonymous concept definition" should "fail if attributes are not defined" in {
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

    val conDef1 = AnonymousConceptDefinition.builder(
      Attribute("attrA", None, Nil) ::
      Attribute("attrB", None, Nil) ::
      Attribute("attrC", None, Nil) :: Nil,
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
    inferredConcept1 should equal(Failure(AttributeExpressionNotFound("attrC")))
  }

  "Anonymous concept" should "inherit attributes from parent concept correctly" in {
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

    val conDef1 = AnonymousConceptDefinition.builder(
      Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil
    ).build

    conDef1.inferAttributeExpressions(context).get should equal (
      AnonymousConceptDefinition.builder(
        Attribute("attr1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .build
    )

    val conDef2 = AnonymousConceptDefinition.builder(
      Nil,
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("cb" :: Nil, "attr1"), IntLiteral(0)))
      .build

    conDef2.inferAttributeExpressions(context).get should equal (
      AnonymousConceptDefinition.builder(
        Attribute("attr1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr1"), IntLiteral(0)))
        .build
    )
  }

}
