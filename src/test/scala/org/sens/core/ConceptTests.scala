package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept._
import org.sens.core.expression.{ConceptAttribute, Variable}
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.concept._
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}
import org.sens.core.expression.operation.logical.And
import org.sens.parser.ValidationContext

class ConceptTests extends AnyFlatSpec with Matchers {
  "Annotations" should "be formatted in Sens correctly" in {
    Annotation("Test", Map()).toSensString should equal ("@Test")

    val ann1 = Annotation("NotNull", Map("defaultValue" -> StringLiteral("empty")))
    ann1.toSensString should equal ("@NotNull (defaultValue = \"empty\")")

    val ann2 = Annotation("Persist", Map("table" -> StringLiteral("orders"), "engine" -> StringLiteral("MySQL")))
    ann2.toSensString should equal ("@Persist (table = \"orders\", engine = \"MySQL\")")
  }

  "Attributes" should "be formatted in Sens correctly" in {
    Attribute("id", None, Nil).toSensString should equal ("id")
    Attribute("val", Some(IntLiteral(1)), Annotation("NotNull", Map()) :: Nil).toSensString should equal ("@NotNull val = 1")
    Attribute("id", None, Annotation("NotNull", Map()) :: Annotation("PrimaryKey", Map()) :: Nil).toSensString should equal ("@NotNull, @PrimaryKey id")
  }

  "Parent concepts" should "be formatted in Sens correctly" in {
    ParentConcept(ConceptReference("myconcept"), None, Map(), Nil).toSensString should equal ("myconcept")
    ParentConcept(ConceptReference("myconcept"), Some("mc"), Map(), Nil).toSensString should equal ("myconcept mc")
    ParentConcept(
      ConceptReference("myconcept"),
      Some("mc"),
      Map("attr1" -> ConceptAttribute("c2" :: Nil, "id"), "attr2" -> ConceptAttribute("c3" :: Nil, "id")),
      Nil).toSensString should equal ("myconcept mc (attr1 = c2.id, attr2 = c3.id)")

    val pc1 = ParentConcept(ConceptReference("myconcept"), Some("mc"), Map(), Annotation("Optional", Map()) :: Nil)
    pc1.toSensString should equal ("@Optional myconcept mc")
    val pc2 = ParentConcept(
      ConceptReference("myconcept"),
      Some("mc"),
      Map(),
      Annotation("Optional", Map()) :: Annotation("Relationship", Map("name" -> StringLiteral("belongsTo"))) :: Nil)
    pc2.toSensString should equal ("@Optional, @Relationship (name = \"belongsTo\") myconcept mc")
  }

  "Concepts" should "correctly return name and attributes" in {
    val conDef1 = Concept.builder(
      "myconcept",
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Annotation.OPTIONAL :: Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map("id" -> ConceptAttribute(Nil, "attr2")), Nil) :: Nil)
      .attributeDependencies(And(
          Equals(ConceptAttribute(Nil, "attr3"), ConceptAttribute("c1" :: Nil, "x1")),
          Equals(ConceptAttribute("c1" :: Nil, "x2"), Variable("attr4"))
      ))
      .build

    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(ValidationContext()) should equal (List("attr1", "attr2", "attr3", "attr4"))
    conDef1.getAttributes(ValidationContext()) should equal (
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) ::
      Attribute("attr2", None, Nil) ::
      Attribute("attr3", None, Nil) ::
      Attribute("attr4", None, Annotation.OPTIONAL :: Nil) :: Nil)
  }

  "Concepts" should "be formatted in Sens correctly" in {
    val conDef1 = Concept(
      "myconcept",
      Nil,
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Some(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id"))),
      ConceptAttribute(Nil, "attr1") :: Nil,
      Some(GreaterThan(ConceptAttribute(Nil, "attr1"), IntLiteral(0))),
      Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Private", Map()) :: Annotation("Persist", Map("table" -> StringLiteral("orders"), "engine" -> StringLiteral("MySQL"))) :: Nil
    )
    val conDef1Str =
      "@Private,\n" +
      "@Persist (table = \"orders\", engine = \"MySQL\")\n" +
      "concept myconcept (attr1 = c1.val)\n" +
      "from concept1 c1, concept2 c2\n" +
      "where (c1.id = c2.concept1id)\n" +
      "group by attr1\n" +
      "having (attr1 > 0)\n" +
      "order by attr1 ASC\n" +
      "limit 10\n" +
      "offset 100"
    conDef1.toSensString should equal(conDef1Str)

    val conDef2 = Concept.builder(
      "myconcept",
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map("id" -> ConceptAttribute("c2" :: Nil, "concept1id")), Nil) ::
      ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
      .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
      .limit(10)
      .build

    val conDef2Str =
      "concept myconcept (attr1 = c1.val)\n" +
      "from concept1 c1 (id = c2.concept1id), concept2 c2\n" +
      "group by attr1\n" +
      "order by attr1 ASC\n" +
      "limit 10"
    conDef2.toSensString should equal(conDef2Str)

    val conDef3 = Concept.builder(
      "myconcept",
      Attribute("attr1", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
          Equals(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute(Nil, "attr1"))
      ))
      .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
      .build

    val conDef3Str =
      "concept myconcept (attr1)\n" +
        "from concept1 c1, concept2 c2\n" +
        "where ((c1.id = c2.concept1id) and (c1.val = attr1))\n" +
        "group by attr1"
    conDef3.toSensString should equal(conDef3Str)

    val conDef4 = Concept.builder(
      "myconcept",
      Attribute("attr1", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
          Equals(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute(Nil, "attr1"))
      ))
      .build

    val conDef4Str =
      "concept myconcept (attr1)\n" +
        "from concept1 c1, concept2 c2\n" +
        "where ((c1.id = c2.concept1id) and (c1.val = attr1))"
    conDef4.toSensString should equal(conDef4Str)
  }
}
