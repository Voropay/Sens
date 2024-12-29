package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, Concept, ParentConcept, AggregationConcept}
import org.sens.core.expression.{ConceptAttribute, ConceptObject}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class AggregationConceptTests extends AnyFlatSpec with Matchers {

  "Aggregation concepts" should "correctly return name and attributes" in {
    val context = ValidationContext()
    context.addConcept(
      ConceptDefinition(Concept.builder(
        "concept1",
        Attribute("id", None, Nil) ::
          Attribute("status", None, Nil) ::
          Attribute("val", None, Nil) ::
          Attribute("timeCreated", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "concept2",
        Attribute("id", None, Nil) ::
          Attribute("status", None, Nil) ::
          Attribute("val", None, Nil) ::
          Attribute("concept1id", None, Nil) ::
          Attribute("timeCreated", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))

    val conDef1 = AggregationConcept.builder(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) ::
        ParentConcept(ConceptReference("concept2"), None, Map(), Annotation.OPTIONAL :: Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("concept2" :: Nil, "concept1id")))
      .build

    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(context) should equal (List("c1", "concept2"))
    conDef1.getAttributes(context) should equal (
      Attribute("c1", Some(ConceptObject("c1")), Nil) ::
        Attribute("concept2", Some(ConceptObject("concept2")), Annotation.OPTIONAL :: Nil) :: Nil
    )
  }

  "Aggregation concepts" should "be formatted in Sens correctly" in {
    val conDef1 = AggregationConcept.builder(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")))
      .annotations(Annotation("Group", Map("name" -> StringLiteral("admin"))) :: Nil)
      .build

    val conDef1Str =
      "@Group (name = \"admin\")\n" +
        "concept myconcept " +
        "between concept1 c1, concept2 c2\n" +
        "where (c1.id = c2.concept1id)"
    conDef1.toSensString should equal(conDef1Str)
  }
}
