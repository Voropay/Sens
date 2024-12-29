package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, IntersectConcept, MinusConcept, ParentConcept, UnionConcept}
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.comparison.Equals
import org.sens.parser.ValidationContext

class UnionConceptTests  extends AnyFlatSpec with Matchers  {

  "Union concepts" should "correctly return name and attributes" in {
    val conDef1 = UnionConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Annotation.OPTIONAL :: Nil) :: Attribute("attr2", None, Nil) :: Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil).build,
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Annotation.OPTIONAL :: Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )
    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2", "attr3"))
    conDef1.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr2", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr3", None, Nil) :: Nil
    )
  }

  "Union concepts" should "be formatted in Sens correctly" in {
    val conDef1 = UnionConcept(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) ::
        ParentConcept(ConceptReference("concept2"), None, Map(), Nil) ::
        ParentConcept(ConceptReference("concept3"), None, Map(), Nil) ::
        Nil,
      Annotation("Group", Map("name" -> StringLiteral("admin"))) :: Nil
    )
    val conDef1Str =
      "@Group (name = \"admin\")\n" +
        "concept myconcept union of\n" +
        "concept1,\n" +
        "concept2,\n" +
        "concept3"

    conDef1.toSensString should equal (conDef1Str)

    val conDef2 = UnionConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil)
        .build(),
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) ::
        Nil,
      Nil
    )
    val conDef2Str =
      "concept myconcept union of\n" +
        "< (attr1, attr2) from concept1 >,\n" +
        "< (attr1 = c2.attr21, attr2 = c2.attr22) from concept2 c2 where (c2.type = \"someType\") >"
    conDef2.toSensString should equal (conDef2Str)
  }

  "Intersect concepts" should "correctly return name and attributes" in {
    val conDef1 = IntersectConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Annotation.OPTIONAL :: Nil) :: Attribute("attr2", None, Nil) :: Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil).build,
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Annotation.OPTIONAL :: Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )
    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2", "attr3"))
    conDef1.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr2", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr3", None, Nil) :: Nil
    )
  }

  "Intersect concepts" should "be formatted in Sens correctly" in {
    val conDef1 = IntersectConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil).build,
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) ::
        Nil,
      Nil
    )
    val conDef1Str =
      "concept myconcept intersect of\n" +
        "< (attr1, attr2) from concept1 >,\n" +
        "< (attr1 = c2.attr21, attr2 = c2.attr22) from concept2 c2 where (c2.type = \"someType\") >"
    conDef1.toSensString should equal (conDef1Str)
  }

  "Minus concepts" should "correctly return name and attributes" in {
    val conDef1 = MinusConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Annotation.OPTIONAL :: Nil) :: Attribute("attr2", None, Nil) :: Attribute("attr3", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil).build,
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Annotation.OPTIONAL :: Nil) ::
            Attribute("attr3", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType")))
          .build,
          None, Map(), Nil) :: Nil,
      Nil
    )
    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2", "attr3"))
    conDef1.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr2", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("attr3", None, Nil) :: Nil
    )
  }

  "Minus concepts" should "be formatted in Sens correctly" in {
    val conDef1 = MinusConcept(
      "myconcept",
      ParentConcept(AnonymousConceptDefinition.builder(
        Attribute("attr1", None, Nil) :: Attribute("attr2", None, Nil) :: Nil,
        ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil).build,
        None, Map(), Nil) ::
        ParentConcept(AnonymousConceptDefinition.builder(
          Attribute("attr1", Some(ConceptAttribute("c2" :: Nil, "attr21")), Nil) ::
            Attribute("attr2", Some(ConceptAttribute("c2" :: Nil, "attr22")), Nil) :: Nil,
          ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "type"), StringLiteral("someType"))).build(),
          None, Map(), Nil) ::
        Nil,
      Nil
    )
    val conDef1Str =
      "concept myconcept minus\n" +
        "< (attr1, attr2) from concept1 >,\n" +
        "< (attr1 = c2.attr21, attr2 = c2.attr22) from concept2 c2 where (c2.type = \"someType\") >"
    conDef1.toSensString should equal (conDef1Str)
  }

}
