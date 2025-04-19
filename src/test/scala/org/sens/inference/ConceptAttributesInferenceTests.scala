package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.parser.ValidationContext
import org.sens.core.statement.ConceptDefinition
import org.sens.core.concept.{Attribute, Concept, ConceptAttributes, ConceptAttributesReference, ConstantIdent, DataSourceConcept, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.literal.StringLiteral

class ConceptAttributesInferenceTests extends AnyFlatSpec with Matchers {

  "Concept Attributes definition" should "infer attributes expressions correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "source",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = ConceptAttributes(
      "someAttrs",
      Nil,
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("source"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "someAttrs",
        Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
          Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("source"), Some("s"), Map(), Nil) :: Nil)
        .build
    )
  }

  "Concept with ConceptAttributesReference" should "infer attributes expressions correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "source",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(ConceptAttributes(
      "someAttrs",
      Nil,
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("source"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )))

    val conDef = Concept.builder(
      "someConcept",
      Attribute("attr3", None, Nil) ::
        ConceptAttributesReference(ConstantIdent("someAttrs"), Map("s" -> StringLiteral("pc"))) :: Nil,
      ParentConcept(ConceptReference("source"), Some("pc"), Map(), Nil) :: Nil
    ).build()

    conDef.inferAttributeExpressions(context).get should equal(
      Concept.builder(
        "someConcept",
        Attribute("attr3", Some(ConceptAttribute("pc" :: Nil, "attr3")), Nil) ::
          Attribute("attr1", Some(ConceptAttribute("pc" :: Nil, "attr1")), Nil) ::
          Attribute("attr2", Some(ConceptAttribute("pc" :: Nil, "attr2")), Nil) :: Nil,
        ParentConcept(ConceptReference("source"), Some("pc"), Map(), Nil) :: Nil)
        .build
    )
  }

}
