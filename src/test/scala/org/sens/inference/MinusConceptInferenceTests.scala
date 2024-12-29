package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Attribute, DataSourceConcept, MinusConcept, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class MinusConceptInferenceTests extends AnyFlatSpec with Matchers {

  "Minus Concept definition" should "infer attributes expressions correctly" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptB",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) :: Nil,
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

    val conDef = MinusConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", None, Nil) ::
            Attribute("attr2", None, Nil) ::
            Attribute("attr3", None, Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("pcC" :: Nil, "attr4"), StringLiteral("someValue")))
          .build(),
          None, Map(), Nil) :: Nil,
      Nil
    )
    conDef.inferAttributeExpressions(context).get should equal (MinusConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("pcB"), Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("pcC" :: Nil, "attr1")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("pcC" :: Nil, "attr2")), Nil) ::
            Attribute("attr3", Some(ConceptAttribute("pcC" :: Nil, "attr3")), Nil) :: Nil,
          ParentConcept(ConceptReference("conceptC"), Some("pcC"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute(List("pcC"), "attr4"), StringLiteral("someValue")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    ))
  }
}