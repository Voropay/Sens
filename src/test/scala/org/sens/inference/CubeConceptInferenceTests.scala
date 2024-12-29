package org.sens.inference

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Annotation, Attribute, Concept, CubeConcept, CubeInheritedConcept, DataSourceConcept, Order, ParentConcept}
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.{ConceptAttribute, FunctionCall}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.comparison.GreaterThan
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.ValidationContext

class CubeConceptInferenceTests extends AnyFlatSpec with Matchers {

  "Cube Concept definition" should "infer attributes expressions correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someConcept",
      Attribute("dim", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("time", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    val conDef = CubeConcept(
      "someCube",
      Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
        Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      Attribute("dim", None, Nil) :: Nil,
      ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    conDef.inferAttributeExpressions(context).get should equal(
      Concept(
        "someCube",
        Nil,
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("dim", Some(ConceptAttribute("c" :: Nil, "dim")), Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
        Some(GreaterThan(ConceptAttribute("c" :: Nil, "time"), StringLiteral("2024-01-01"))),
        ConceptAttribute(Nil, "dim") :: Nil,
        Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
        Some(10),
        Some(100),
        Annotation("Public", Map()) :: Nil
      )
    )
  }

  "Cube Inherited Concept definition" should "infer attributes expressions correctly" in {
    val context = ValidationContext()

    context.addConcept(ConceptDefinition(DataSourceConcept(
      "someConcept",
      Attribute("category", None, Nil) ::
        Attribute("country", None, Nil) ::
        Attribute("val", None, Nil) ::
        Attribute("date", None, Nil) :: Nil,
      FileDataSource("someFile", FileFormats.CSV),
      Nil
    )))

    context.addConcept(ConceptDefinition(
      CubeConcept.builder(
        "someCube",
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("avgVal", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) :: Nil,
        Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil)
        .annotations(Annotation("Private", Map()) :: Nil).build()
    ))

    val conDef = CubeInheritedConcept(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
      Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "category") :: Nil,
      Some(GreaterThan(ConceptAttribute("c" :: "c" :: Nil, "date"), StringLiteral("2024-01-01"))),
      Some(GreaterThan(ConceptAttribute("c" :: Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute("c" :: Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Public", Map()) :: Nil
    )

    conDef.inferAttributeExpressions(context).get should equal(
      Concept(
        "anotherCube",
        Nil,
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("date", Some(ConceptAttribute("c" :: Nil, "date")), Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
        Some(GreaterThan(ConceptAttribute("c" :: Nil, "date"), StringLiteral("2024-01-01"))),
        ConceptAttribute(Nil, "date") :: Nil,
        Some(GreaterThan(ConceptAttribute(Nil, "totalVal"), IntLiteral(100))),
        Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
        Some(10),
        Some(100),
        Annotation("Public", Map()) :: Nil
      )
    )

    context.addConcept(ConceptDefinition(conDef))

    val conDef1 = CubeInheritedConcept.builder(
      "yetAnotherCube",
      ParentConcept(ConceptReference("anotherCube"), Some("c"), Map(), Nil)
    ).overriddenDimensions(Attribute("country", None, Nil) :: Nil)
      .build()

    conDef1.inferAttributeExpressions(context).get should equal(
      Concept(
        "yetAnotherCube",
        Nil,
        Attribute("totalVal", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: Nil, "val") :: Nil)), Nil) ::
          Attribute("date", Some(ConceptAttribute("c" :: Nil, "date")), Nil) ::
          Attribute("country", Some(ConceptAttribute("c" :: Nil, "country")), Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c"), Map(), Nil) :: Nil,
        Some(GreaterThan(ConceptAttribute("c" :: Nil, "date"), StringLiteral("2024-01-01"))),
        ConceptAttribute(Nil, "date") :: ConceptAttribute(Nil, "country") :: Nil,
        None,
        Nil,
        None,
        None,
        Nil
      )
    )
  }
}
