package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Attribute, Concept, DataSourceConcept, InheritedConcept, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.{AttributeExpressionNotFound, ValidationContext}

import scala.util.Failure

class InheritedConceptInferenceTests extends AnyFlatSpec with Matchers {

  "Inherited Concept definition" should "infer attributes expressions correctly" in {
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

    val conDef1 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .build

    conDef1.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr1", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .build
    )

    val conDef2 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
      .removedAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
    .build

    conDef2.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) ::
        Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .build
    )

    val conDef3 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr1" -> ConceptAttribute(Nil, "id")), Nil) :: Nil)
      .overriddenAttributes(
        Attribute("id", None, Nil) ::
          Attribute("val", None, Nil) ::Nil)
      .removedAttributes(
        ConceptAttribute(Nil, "attr1") ::
          ConceptAttribute("cb" :: Nil, "attr2") :: Nil)
      .additionalDependencies(And(
        Equals(ConceptAttribute(Nil, "val"), ConceptAttribute("cb" :: Nil, "attr2")),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(0))
      ))
      .build

    conDef3.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr3", Some(ConceptAttribute("cb" :: Nil, "attr3")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) ::
        Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
        .attributeDependencies(GreaterThan(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(0)))
        .build
    )
  }

  "Inherited Concept definition" should "infer attributes expressions correctly for joins" in {
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

    val conDef1 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) :: Nil)
      .overriddenAttributes(
        Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
          Attribute("val", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil)
      .removedAttributes(ConceptAttribute(Nil, "attr1") :: ConceptAttribute("cb" :: Nil, "attr3") :: Nil)
      .additionalDependencies(Equals(ConceptAttribute("cb" :: Nil, "attr1"), StringLiteral("someValue")))
      .build

    conDef1.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("attr2", Some(ConceptAttribute("cb" :: Nil, "attr2")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("cb" :: Nil, "attr4")), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("cc" :: Nil, "attr3")), Nil) ::
        Attribute("id", Some(ConceptAttribute("cb" :: Nil, "attr1")), Nil) ::
        Attribute("val", Some(ConceptAttribute("cc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptC"), Some("cc"), Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1")), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute(List("cb"), "attr1"), StringLiteral("someValue")))
        .build
    )
  }

  "Attribute inference of inherited concept definition" should "fail if attributes are not defined" in {
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

    val conDef1 = InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("id", None, Nil) :: Nil)
      .build
    conDef1.inferAttributeExpressions(context) should equal(Failure(AttributeExpressionNotFound("id")))
  }

}
