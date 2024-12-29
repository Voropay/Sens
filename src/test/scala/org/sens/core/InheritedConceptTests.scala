package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, Concept, InheritedConcept, Order, ParentConcept}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, NamedElementPlaceholder}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.expression.operation.logical.And
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class InheritedConceptTests extends AnyFlatSpec with Matchers {

  "Inherited concepts" should "correctly return name and attributes" in {
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
          Attribute("status", None, Annotation.OPTIONAL :: Nil) ::
          Attribute("val", None, Nil) ::
          Attribute("concept1id", None, Nil) ::
          Attribute("timeCreated", None, Annotation.OPTIONAL :: Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))

    val conDef1 = InheritedConcept(
      "myconcept",
      Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Attribute("val", None, Annotation.OPTIONAL :: Nil) :: Attribute("timeCreated", Some(ConceptAttribute("c2" :: Nil, "timeCreated")), Nil) :: Nil,
      ConceptAttribute("c1" :: Nil, "id") :: ConceptAttribute("c2" :: Nil, "id") :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute(Nil, "val"),
          Add(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute("c2" :: Nil, "val")))
      )),
      Order(ConceptAttribute(Nil, "timeCreated"), Order.DESC) :: Nil,
      Some(20),
      Some(100),
      Nil
    )
    conDef1.name should equal ("myconcept")
    conDef1.getAttributeNames(context) should equal (List("status", "concept1id", "val", "timeCreated"))
    conDef1.getAttributes(context) should equal (
      Attribute("status", Some(ConceptAttribute("c1" :: Nil, "status")), Nil) ::
        Attribute("concept1id", Some(ConceptAttribute("c2" :: Nil, "concept1id")), Nil) ::
        Attribute("val", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("timeCreated", Some(ConceptAttribute("c2" :: Nil, "timeCreated")), Nil) :: Nil
    )

    val conDef2 = InheritedConcept.builder(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("type", Some(StringLiteral("someVal")), Nil) :: Nil)
      .additionalDependencies(Equals(NamedElementPlaceholder("status"), StringLiteral("someValue")))
      .orderByAttributes(Order(ConceptAttribute(Nil, "timeCreated"), Order.DESC) :: Nil)
      .limit(20)
      .offset(100)
      .build

    conDef2.name should equal ("myconcept")
    conDef2.getAttributeNames(context) should equal (List("id", "status", "val", "timeCreated", "type"))
    conDef2.getAttributes(context) should equal (
      Attribute("id", Some(ConceptAttribute("concept1" :: Nil, "id")), Nil) ::
        Attribute("status", Some(ConceptAttribute("concept1" :: Nil, "status")), Nil) ::
        Attribute("val", Some(ConceptAttribute("concept1" :: Nil, "val")), Nil) ::
        Attribute("timeCreated", Some(ConceptAttribute("concept1" :: Nil, "timeCreated")), Nil) ::
        Attribute("type", Some(StringLiteral("someVal")), Nil) :: Nil
    )

    val conDef3 = InheritedConcept(
      "myconcept",
      Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Attribute("val", None, Nil) :: Attribute("timeCreated", Some(ConceptAttribute("c2" :: Nil, "timeCreated")), Nil) :: Nil,
      ConceptAttribute(Nil, "id") :: ConceptAttribute("c1" :: Nil, "status") :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute(Nil, "val"),
          Add(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute("c2" :: Nil, "val")))
      )),
      Order(ConceptAttribute(Nil, "timeCreated"), Order.DESC) :: Nil,
      Some(20),
      Some(100),
      Nil
    )
    conDef3.getAttributeNames(context) should equal (List("status", "concept1id", "val", "timeCreated"))
    conDef3.getAttributes(context) should equal (
      Attribute("status", Some(ConceptAttribute("c2" :: Nil, "status")), Annotation.OPTIONAL :: Nil) ::
        Attribute("concept1id", Some(ConceptAttribute("c2" :: Nil, "concept1id")), Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("timeCreated", Some(ConceptAttribute("c2" :: Nil, "timeCreated")), Nil) :: Nil
    )
  }

  "Inherited concepts" should "be formatted in Sens correctly" in {
    val conDef1 = InheritedConcept(
      "myconcept",
      Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Attribute("val", None, Nil) :: Nil,
      ConceptAttribute("c1" :: Nil, "id") :: ConceptAttribute("c2" :: Nil, "id") :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute(Nil, "val"),
          Add(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute("c2" :: Nil, "val")))
      )),
      Order(ConceptAttribute(Nil, "timeCreated"), Order.DESC) :: Nil,
      Some(20),
      Some(100),
      Annotation("Group", Map("name" -> StringLiteral("admin"))) :: Nil
    )
    val conDef1Str =
      "@Group (name = \"admin\")\n" +
        "concept myconcept is concept1 c1, concept2 c2\n" +
        "with val\n" +
        "without c1.id, c2.id\n" +
        "where ((c1.id = c2.concept1id) and (val = (c1.val + c2.val)))\n" +
        "order by timeCreated DESC\n" +
        "limit 20\n" +
        "offset 100"
    conDef1.toSensString should equal(conDef1Str)

    val conDef2 = InheritedConcept.builder(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("val", None, Nil) :: Nil)
      .additionalDependencies(Equals(
        ConceptAttribute(Nil, "val"),
        FunctionCall(FunctionReference("convert"), ConceptAttribute("c1" :: Nil, "val") :: Nil)
      ))
      .limit(20)
      .build

    val conDef2Str =
      "concept myconcept is concept1 c1\n" +
        "with val\n" +
        "where (val = convert(c1.val))\n" +
        "limit 20"
    conDef2.toSensString should equal(conDef2Str)

    val conDef3 = InheritedConcept.builder(
      "myconcept",
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil)
      .additionalDependencies(Equals(
        ConceptAttribute(Nil, "status"),
        StringLiteral("ACTIVE")))
      .build

    val conDef3Str =
      "concept myconcept is concept1\n" +
        "where (status = \"ACTIVE\")"
    conDef3.toSensString should equal(conDef3Str)
  }
}
