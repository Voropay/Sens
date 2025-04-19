package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, GenericParameter}
import org.sens.core.expression.concept.{ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.StringLiteral
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class ConceptAttributesTests extends AnyFlatSpec with Matchers {

  "Concept Attributes" should "be formatted in Sens correctly" in {
    val conDef = ConceptAttributes(
      "someAttrs",
      "source" :: Nil,
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(GenericConceptReference(ExpressionIdent(GenericParameter("source")), Map()), Some("s"), Map(), Nil) :: Nil,
      Annotation("Description", Map("text" -> StringLiteral("Dimension keys"))) :: Nil
    )

    val conDefStr =
      "@Description (text = \"Dimension keys\")\n" +
        "concept attributes someAttrs[source] (attr1 = s.attr1,\n" +
        "attr2 = s.attr2)\n" +
        "from $source s"
    conDef.toSensString should equal(conDefStr)
  }

  "Concept Attributes" should "correctly return name and attributes" in {
    val conDef = ConceptAttributes(
      "someAttrs",
      Nil,
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("source"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )

    conDef.name should equal("someAttrs")
    conDef.getAttributeNames(ValidationContext()) should equal(List("attr1", "attr2"))
    conDef.getAttributes(ValidationContext()) should equal(
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil)
  }

  "Concept" should "correctly return name and attributes for ConceptAttributesReference" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptDataSource1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptDataSource2",
      Attribute("val1", None, Nil) ::
        Attribute("val2", None, Nil) :: Nil,
      FileDataSource("someFile2", FileFormats.CSV),
      Nil
    )))
    context.addConcept(ConceptDefinition(ConceptAttributes(
     "attributesList",
      Nil,
      Attribute("attr1", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptDataSource1"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )))

    val conDef = Concept.builder(
      "myConcept",
      Attribute("val1", Some(ConceptAttribute("s2" :: Nil, "val1")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("s2" :: Nil, "val2")), Nil) ::
        ConceptAttributesReference(ConstantIdent("attributesList"), Map("s" -> StringLiteral("s1"))) :: Nil,
      ParentConcept(ConceptReference("conceptDataSource1"), Some("s1"), Map(), Nil) ::
        ParentConcept(ConceptReference("conceptDataSource2"), Some("s2"), Map("val1" -> ConceptAttribute("s1" :: Nil, "attr1")), Nil) :: Nil
    ).build()

    conDef.getAttributeNames(context) should equal(List("val1", "val2", "attr1", "attr2"))
    conDef.getAttributes(context) should equal(
      Attribute("val1", Some(ConceptAttribute("s2" :: Nil, "val1")), Nil) ::
        Attribute("val2", Some(ConceptAttribute("s2" :: Nil, "val2")), Nil) ::
        Attribute("attr1", Some(ConceptAttribute("s1" :: Nil, "attr1")), Nil) ::
        Attribute("attr2", Some(ConceptAttribute("s1" :: Nil, "attr2")), Nil) :: Nil)
  }

  "Cube Concept" should "correctly return name and attributes for ConceptAttributesReference" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(DataSourceConcept(
      "conceptDataSource1",
      Attribute("attr1", None, Nil) ::
        Attribute("attr2", None, Nil) ::
        Attribute("attr3", None, Nil) ::
        Attribute("attr4", None, Nil) :: Nil,
      FileDataSource("someFile1", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(ConceptAttributes(
      "dimensionsList",
      Nil,
      Attribute("attr3", Some(ConceptAttribute("s" :: Nil, "attr1")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("s" :: Nil, "attr2")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptDataSource1"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )))

    context.addConcept(ConceptDefinition(ConceptAttributes(
      "metricsList",
      Nil,
      Attribute("attr1Sum", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("s" :: Nil, "attr1") :: Nil)), Nil)  ::
        Attribute("attr2Sum", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("s" :: Nil, "attr2") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptDataSource1"), Some("s"), Map(), Nil) :: Nil,
      Nil
    )))

    val conDef = CubeConcept.builder(
      "someCube",
      ConceptAttributesReference(ConstantIdent("metricsList"), Map("s" -> StringLiteral("source"))) ::
        Attribute("attr1Avg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("source" :: Nil, "attr1") :: Nil)), Nil) ::
        Attribute("attr2Avg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("source" :: Nil, "attr2") :: Nil)), Nil) :: Nil,
      ConceptAttributesReference(ConstantIdent("dimensionsList"), Map("s" -> StringLiteral("source"))) :: Nil,
      ParentConcept(ConceptReference("conceptDataSource1"), Some("source"), Map(), Nil) :: Nil)
      .build()

    conDef.getAttributeNames(context) should equal(List("attr1Sum", "attr2Sum", "attr1Avg", "attr2Avg", "attr3", "attr4"))
    conDef.getAttributes(context) should equal(
      Attribute("attr1Sum", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("source" :: Nil, "attr1") :: Nil)), Nil) ::
        Attribute("attr2Sum", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("source" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("attr1Avg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("source" :: Nil, "attr1") :: Nil)), Nil) ::
        Attribute("attr2Avg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("source" :: Nil, "attr2") :: Nil)), Nil) ::
        Attribute("attr3", Some(ConceptAttribute("source" :: Nil, "attr1")), Nil) ::
        Attribute("attr4", Some(ConceptAttribute("source" :: Nil, "attr2")), Nil) :: Nil
    )

  }

}

