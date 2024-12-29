package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, CubeConcept, CubeInheritedConcept, Order, ParentConcept}
import org.sens.core.expression.{ConceptAttribute, FunctionCall}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.GreaterThan
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class CubeConceptTests extends AnyFlatSpec with Matchers {

  "Cube concepts" should "correctly return name and attributes" in {
    val context = ValidationContext()
    val conDef = CubeConcept.builder(
      "someCube",
      Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil)
      .attributeDependencies(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01")))
      .build()

    conDef.name should equal("someCube")
    conDef.getAttributeNames(context) should equal(List("totalVal", "avgVal", "category"))
    conDef.getAttributes(context) should equal(
      Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil
    )
  }

  "Cube concepts" should "be formatted in Sens correctly" in {
    val conDef = CubeConcept(
      "someCube",
      Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil,
      ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    val conDefStr =
      "@Public\n" +
        "concept cube someCube\n" +
        "metrics (totalVal = sum(c.val),\n" +
        "avgVal = avg(c.val))\n" +
        "dimensions (category = c.category)\n" +
        "from someConcept c\n" +
        "where (c.time > \"2024-01-01\")\n" +
        "having (totalVal > 100)\n" +
        "order by totalVal ASC\n" +
        "limit 10\n" +
        "offset 100"
    conDef.toSensString should equal(conDefStr)
  }

  "Cube inherited concepts" should "correctly return name and attributes" in {
    val context = ValidationContext()
    context.addConcept(ConceptDefinition(
      CubeConcept.builder(
        "someCube",
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
        Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil)
        .attributeDependencies(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01")))
        .build()
    ))

    val conDef1 = CubeInheritedConcept.builder(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil)).build()



    conDef1.name should equal("anotherCube")
    conDef1.getAttributeNames(context) should equal(List("totalVal", "avgVal", "category"))
    conDef1.getAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(ConceptAttribute("c" :: Nil, "avgVal")), Nil) ::
        Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil
    )
    conDef1.getCurrentAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(ConceptAttribute("c" :: Nil, "avgVal")), Nil) ::
        Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil
    )
    conDef1.getMetrics(context) should equal (
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(ConceptAttribute("c" :: Nil, "avgVal")), Nil) :: Nil
    )
    conDef1.getDimensions(context) should equal (
      Attribute("category", Some(ConceptAttribute("c" :: Nil, "category")), Nil) :: Nil
    )

    val conDef2 = CubeInheritedConcept.builder(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil))
      .overriddenMetrics(
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: "c" :: Nil, "val1") :: Nil)), Nil) ::
          Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil
      )
      .overriddenDimensions(
        Attribute("category", Some(ConceptAttribute("c" :: "c" :: Nil, "category1")), Nil) ::
          Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
      )
      .build()

    conDef2.getAttributeNames(context) should equal(List("totalVal", "avgVal", "maxVal", "category", "date"))
    conDef2.getAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: "c" :: Nil, "val1") :: Nil)), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("category", Some(ConceptAttribute("c" :: "c" :: Nil, "category1")), Nil) ::
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )
    conDef2.getCurrentAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: "c" :: Nil, "val1") :: Nil)), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("category", Some(ConceptAttribute("c" :: "c" :: Nil, "category1")), Nil) ::
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )
    conDef2.getMetrics(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: "c" :: Nil, "val1") :: Nil)), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil
    )
    conDef2.getDimensions(context) should equal(
      Attribute("category", Some(ConceptAttribute("c" :: "c" :: Nil, "category1")), Nil) ::
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )

    val conDef3 = CubeInheritedConcept.builder(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil))
      .overriddenMetrics(
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil
      )
      .removedMetrics(ConceptAttribute("c" :: Nil, "avgVal") :: Nil)
      .overriddenDimensions(
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
      )
      .removedDimensions(ConceptAttribute("c" :: Nil, "category") :: Nil)
      .build()

    conDef3.getAttributeNames(context) should equal(List("totalVal", "maxVal", "date"))
    conDef3.getAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )
    conDef3.getCurrentAttributes(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )
    conDef3.getMetrics(context) should equal(
      Attribute("totalVal", Some(ConceptAttribute("c" :: Nil, "totalVal")), Nil) ::
        Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil
    )
    conDef3.getDimensions(context) should equal(
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil
    )
  }

  "Cube inherited concepts" should "be formatted in Sens correctly" in {
    val conDef = CubeInheritedConcept(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
      Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "category") :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: "c" :: Nil, "date"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    val conDefStr =
      "@Public\n" +
        "concept cube anotherCube " +
        "is someCube c\n" +
        "with metrics maxVal = max(c.c.val)\n" +
        "without metrics c.avgVal\n" +
        "with dimensions date = c.c.date\n" +
        "without dimensions c.category\n" +
        "where (c.c.date > \"2024-01-01\")\n" +
        "having (totalVal > 100)\n" +
        "order by totalVal ASC\n" +
        "limit 10\n" +
        "offset 100"
    conDef.toSensString should equal(conDefStr)
  }

}
