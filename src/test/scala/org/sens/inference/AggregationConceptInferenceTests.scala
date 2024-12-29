package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{AggregationConcept, Attribute, Concept, DataSourceConcept, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.{ConceptAttribute, ConceptObject}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class AggregationConceptInferenceTests extends AnyFlatSpec with Matchers {

  "Aggregation Concept definition" should "infer attributes expressions correctly" in {
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

    val conDef = AggregationConcept(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), Some("cb"), Map("attr1" -> ConceptAttribute("cc" :: Nil, "attr1")), Nil) ::
      ParentConcept(ConceptReference("conceptC"), Some("cc"), Map(), Nil) :: Nil,
      Some(And(
        And(
          Equals(ConceptAttribute("cb" :: Nil, "attr2"), StringLiteral("SomeValue")),
          Equals(ConceptAttribute("cc" :: Nil, "attr2"), StringLiteral("SomeValue"))
        ),
        GreaterThan(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(0))
      )),
      Nil
    )
    conDef.inferAttributeExpressions(context).get should equal (
      Concept.builder(
        "conceptA",
        Attribute("cb", Some(ConceptObject("cb")), Nil) ::
        Attribute("cc", Some(ConceptObject("cc")), Nil) :: Nil,
        ParentConcept(ConceptReference("conceptB"), Some("cb"), Map(), Nil) ::
        ParentConcept(
          ConceptReference("conceptC"),
          Some("cc"),
          Map("attr1" -> ConceptAttribute("cb" :: Nil, "attr1"), "attr2" -> StringLiteral("SomeValue")),
          Nil) :: Nil)
        .attributeDependencies(And(
          Equals(ConceptAttribute(List("cb"), "attr2"), StringLiteral("SomeValue")),
          GreaterThan(ConceptAttribute("cb" :: Nil, "attr3"), IntLiteral(0))))
        .build
    )
  }
}
