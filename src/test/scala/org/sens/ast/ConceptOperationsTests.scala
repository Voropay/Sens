package org.sens.ast

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept._
import org.sens.core.expression.literal.{BooleanLiteral, FloatLiteral, IntLiteral, StringLiteral}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, GenericParameter, Variable}
import org.sens.parser.WrongTypeException
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.expression.concept.{ConceptReference, GenericConceptReference}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.operation.arithmetic.Multiply
import org.sens.core.expression.operation.comparison.{Equals, GreaterThan}

class ConceptOperationsTests extends AnyFlatSpec with Matchers {

  "Concept elements" should "work with subexpressions correctly" in {
    val an = Annotation(
      "someAnnotation",
      Map("name" -> StringLiteral("value"))
    )
    an.getSubExpressions should equal(StringLiteral("value") :: Nil)
    an.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    an.findSubExpression(_ == StringLiteral("value")) should equal(Some(StringLiteral("value")))
    an.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    an.findAllSubExpressions(_ == StringLiteral("value")) should equal(List(StringLiteral("value")))
    an.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(an)
    an.replaceSubExpression(StringLiteral("value"), IntLiteral(1)) should equal(
      Annotation(
        "someAnnotation",
        Map("name" -> IntLiteral(1))
      )
    )
    assertThrows[WrongTypeException] {
      an.replaceSubExpression(StringLiteral("value"), Variable("a"))
    }

    val at = Attribute(
      "name",
      Some(StringLiteral("N/A")),
      an :: Nil
    )
    at.getSubExpressions should equal(StringLiteral("N/A") :: Nil)
    at.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    at.findSubExpression(_ == StringLiteral("N/A")) should equal(Some(StringLiteral("N/A")))
    at.findSubExpression(_ == StringLiteral("value")) should equal(Some(StringLiteral("value")))
    at.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    at.findAllSubExpressions(_ == StringLiteral("N/A")) should equal(List(StringLiteral("N/A")))
    at.findAllSubExpressions(_ == StringLiteral("value")) should equal(List(StringLiteral("value")))
    at.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(at)
    at.replaceSubExpression(StringLiteral("N/A"), IntLiteral(1)) should equal(
      Attribute(
        "name",
        Some(IntLiteral(1)),
        an :: Nil
      )
    )
    at.replaceSubExpression(StringLiteral("value"), IntLiteral(1)) should equal(
      Attribute(
        "name",
        Some(StringLiteral("N/A")),
        Annotation(
          "someAnnotation",
          Map("name" -> IntLiteral(1))
        ) :: Nil
      )
    )

    val o = Order(ConceptAttribute(Nil, "a"), Order.DESC)
    o.getSubExpressions should equal(ConceptAttribute(Nil, "a") :: Nil)
    o.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    o.findSubExpression(_ == ConceptAttribute(Nil, "a")) should equal(Some(ConceptAttribute(Nil, "a")))
    o.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    o.findAllSubExpressions(_ == ConceptAttribute(Nil, "a")) should equal(List(ConceptAttribute(Nil, "a")))
    o.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(o)
    o.replaceSubExpression(ConceptAttribute(Nil, "a"), IntLiteral(1)) should equal(
      Order(IntLiteral(1), Order.DESC)
    )

    val p = ParentConcept(
      ConceptReference("someConcept"),
      None,
      Map("attr" -> IntLiteral(0)),
      an :: Nil
    )
    p.getSubExpressions should equal(ConceptReference("someConcept") :: IntLiteral(0) :: Nil)
    p.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    p.findSubExpression(_ == ConceptReference("someConcept")) should equal(Some(ConceptReference("someConcept")))
    p.findSubExpression(_ == IntLiteral(0)) should equal(Some(IntLiteral(0)))
    p.findSubExpression(_ == StringLiteral("value")) should equal(Some(StringLiteral("value")))
    p.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    p.findAllSubExpressions(_ == ConceptReference("someConcept")) should equal(List(ConceptReference("someConcept")))
    p.findAllSubExpressions(_ == IntLiteral(0)) should equal(List(IntLiteral(0)))
    p.findAllSubExpressions(_ == StringLiteral("value")) should equal(List(StringLiteral("value")))
    p.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(p)
    p.replaceSubExpression(ConceptReference("someConcept"), ConceptReference("anotherConcept")) should equal(
      ParentConcept(
        ConceptReference("anotherConcept"),
        None,
        Map("attr" -> IntLiteral(0)),
        an :: Nil
      )
    )
    p.replaceSubExpression(IntLiteral(0), IntLiteral(1)) should equal(
      ParentConcept(
        ConceptReference("someConcept"),
        None,
        Map("attr" -> IntLiteral(1)),
        an :: Nil
      )
    )
    p.replaceSubExpression(StringLiteral("value"), BooleanLiteral(true)) should equal(
      ParentConcept(
        ConceptReference("someConcept"),
        None,
        Map("attr" -> IntLiteral(0)),
        Annotation(
          "someAnnotation",
          Map("name" -> BooleanLiteral(true))
        ) :: Nil
      )
    )

    val fds = FileDataSource("path", FileFormats.CSV)
    fds.getSubExpressions should equal(Nil)
    fds.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    fds.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    fds.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(fds)

    val eid = ExpressionIdent(GenericParameter("param"))
    eid.getSubExpressions should equal (GenericParameter("param") :: Nil)
    eid.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    eid.findSubExpression(_ == GenericParameter("param")) should equal(Some(GenericParameter("param")))
    eid.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    eid.findAllSubExpressions(_ == GenericParameter("param")) should equal(List(GenericParameter("param")))
    eid.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(eid)
    eid.replaceSubExpression(GenericParameter("param"), StringLiteral("val")) should equal(
      ExpressionIdent(StringLiteral("val"))
    )

    val cid = ConstantIdent("name")
    cid.getSubExpressions should equal(Nil)
    cid.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    cid.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    cid.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(cid)

    val gcr = GenericConceptReference(ExpressionIdent(GenericParameter("name")), Map("param" -> GenericParameter("paramVal")))
    gcr.getSubExpressions should equal(GenericParameter("paramVal") :: Nil)
    gcr.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    gcr.findSubExpression(_ == GenericParameter("name")) should equal(Some(GenericParameter("name")))
    gcr.findSubExpression(_ == GenericParameter("paramVal")) should equal(Some(GenericParameter("paramVal")))
    gcr.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    gcr.findAllSubExpressions(_ == GenericParameter("name")) should equal(List(GenericParameter("name")))
    gcr.findAllSubExpressions(_ == GenericParameter("paramVal")) should equal(List(GenericParameter("paramVal")))
    gcr.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(gcr)
    gcr.replaceSubExpression(GenericParameter("name"), StringLiteral("name")) should equal(
      GenericConceptReference(ExpressionIdent(StringLiteral("name")), Map("param" -> GenericParameter("paramVal")))
    )
    gcr.replaceSubExpression(GenericParameter("paramVal"), StringLiteral("someVal")) should equal(
      GenericConceptReference(ExpressionIdent(GenericParameter("name")), Map("param" -> StringLiteral("someVal")))
    )

    val car = ConceptAttributesReference(ExpressionIdent(GenericParameter("name")), Map("source" -> GenericParameter("s")))
    car.getSubExpressions should equal(GenericParameter("s") :: Nil)
    car.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    car.findSubExpression(_ == GenericParameter("name")) should equal(Some(GenericParameter("name")))
    car.findSubExpression(_ == GenericParameter("s")) should equal(Some(GenericParameter("s")))
    car.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    car.findAllSubExpressions(_ == GenericParameter("name")) should equal(List(GenericParameter("name")))
    car.findAllSubExpressions(_ == GenericParameter("s")) should equal(List(GenericParameter("s")))
    car.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(car)
    car.replaceSubExpression(GenericParameter("name"), StringLiteral("name")) should equal(
      ConceptAttributesReference(ExpressionIdent(StringLiteral("name")), Map("source" -> GenericParameter("s")))
    )
    car.replaceSubExpression(GenericParameter("s"), StringLiteral("p")) should equal(
      ConceptAttributesReference(ExpressionIdent(GenericParameter("name")), Map("source" -> StringLiteral("p")))
    )
  }

  "Concept" should "work with subexpressions correctly" in {
    val c = Concept(
      "avgPrices",
      Nil,
      Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
        Attribute("priceAvg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil,
      ParentConcept(ConceptReference("prices"), Some("p"), Map(), Nil) :: Nil,
      Some(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca"))),
      ConceptAttribute("p" :: Nil, "category") :: ConceptAttribute(Nil, "date") :: Nil,
      Some(GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(10))),
      Order(ConceptAttribute(Nil, "category"), Order.DESC) :: Nil,
      None,
      None,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    c.getSubExpressions should equal (
      Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca")) ::
        ConceptAttribute("p" :: Nil, "category") :: ConceptAttribute(Nil, "date") ::
        GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(10)) :: Nil)

    c.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    c.findSubExpression(_ == ConceptAttribute("p" :: Nil, "price")) should equal (Some(ConceptAttribute("p" :: Nil, "price")))
    c.findSubExpression(_ == ConceptReference("prices")) should equal (Some(ConceptReference("prices")))
    c.findSubExpression(_ == StringLiteral("ca")) should equal (Some(StringLiteral("ca")))
    c.findSubExpression(_ == FloatLiteral(10)) should equal (Some(FloatLiteral(10)))
    c.findSubExpression(_ == ConceptAttribute(Nil, "category")) should equal (Some(ConceptAttribute(Nil, "category")))
    c.findSubExpression(_ == ConceptAttribute(Nil, "date")) should equal (Some(ConceptAttribute(Nil, "date")))
    c.findSubExpression(_ == StringLiteral("view")) should equal (Some(StringLiteral("view")))

    c.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    c.findAllSubExpressions(_ == ConceptAttribute("p" :: Nil, "price")) should equal(List(ConceptAttribute("p" :: Nil, "price")))
    c.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    c.findAllSubExpressions(_ == StringLiteral("ca")) should equal(List(StringLiteral("ca")))
    c.findAllSubExpressions(_ == FloatLiteral(10)) should equal(List(FloatLiteral(10)))
    c.findAllSubExpressions(_ == ConceptAttribute(Nil, "category")) should equal(List(ConceptAttribute(Nil, "category")))
    c.findAllSubExpressions(_ == ConceptAttribute(Nil, "date")) should equal(List(ConceptAttribute(Nil, "date")))
    c.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    c.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(c)
    c.replaceSubExpression(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("p" :: Nil, "categoryName")) should equal(
      c.copy(
        groupByAttributes = ConceptAttribute("p" :: Nil, "categoryName") :: ConceptAttribute(Nil, "date") :: Nil,
        attributes = Attribute("category", Some(ConceptAttribute("p" :: Nil, "categoryName")), Nil) ::
          Attribute("priceAvg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil)
    )
    c.replaceSubExpression(ConceptReference("prices"), ConceptReference("prices1")) should equal(
      c.copy(parentConcepts = ParentConcept(ConceptReference("prices1"), Some("p"), Map(), Nil) :: Nil)
    )
    c.replaceSubExpression(StringLiteral("ca"), StringLiteral("us")) should equal(
      c.copy(attributeDependencies = Some(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("us"))))
    )
    c.replaceSubExpression(FloatLiteral(10), FloatLiteral(20)) should equal(
      c.copy(groupDependencies = Some(GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(20))))
    )
    c.replaceSubExpression(ConceptAttribute(Nil, "category"), ConceptAttribute(Nil, "categoryName")) should equal(
      c.copy(orderByAttributes = Order(ConceptAttribute(Nil, "categoryName"), Order.DESC) :: Nil)
    )
    c.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      c.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "CubeConcept" should "work with subexpressions correctly" in {
    val c = CubeConcept(
      "avgPrices",
      Attribute("priceAvg", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil,
      Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
        Attribute("date", Some(ConceptAttribute("p" :: Nil, "date")), Nil) :: Nil,
      ParentConcept(ConceptReference("prices"), Some("p"), Map(), Nil) :: Nil,
      Some(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca"))),
      Some(GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(10))),
      Order(ConceptAttribute(Nil, "category"), Order.DESC) :: Nil,
      None,
      None,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    c.getSubExpressions should equal(
      Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("ca")) ::
        GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(10)) :: Nil)

    c.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    c.findSubExpression(_ == ConceptAttribute("p" :: Nil, "price")) should equal(Some(ConceptAttribute("p" :: Nil, "price")))
    c.findSubExpression(_ == ConceptAttribute("p" :: Nil, "category")) should equal(Some(ConceptAttribute("p" :: Nil, "category")))
    c.findSubExpression(_ == ConceptAttribute("p" :: Nil, "date")) should equal(Some(ConceptAttribute("p" :: Nil, "date")))
    c.findSubExpression(_ == ConceptReference("prices")) should equal(Some(ConceptReference("prices")))
    c.findSubExpression(_ == StringLiteral("ca")) should equal(Some(StringLiteral("ca")))
    c.findSubExpression(_ == FloatLiteral(10)) should equal(Some(FloatLiteral(10)))
    c.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    c.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    c.findAllSubExpressions(_ == ConceptAttribute("p" :: Nil, "price")) should equal(List(ConceptAttribute("p" :: Nil, "price")))
    c.findAllSubExpressions(_ == ConceptAttribute("p" :: Nil, "category")) should equal(List(ConceptAttribute("p" :: Nil, "category")))
    c.findAllSubExpressions(_ == ConceptAttribute("p" :: Nil, "date")) should equal(List(ConceptAttribute("p" :: Nil, "date")))
    c.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    c.findAllSubExpressions(_ == StringLiteral("ca")) should equal(List(StringLiteral("ca")))
    c.findAllSubExpressions(_ == FloatLiteral(10)) should equal(List(FloatLiteral(10)))
    c.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    c.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(c)
    c.replaceSubExpression(FunctionReference("avg"), FunctionReference("sum")) should equal(
      c.copy(
        metrics = Attribute("priceAvg", Some(FunctionCall(FunctionReference("sum"), ConceptAttribute("p" :: Nil, "price") :: Nil)), Nil) :: Nil)
    )
    c.replaceSubExpression(ConceptAttribute("p" :: Nil, "category"), ConceptAttribute("p" :: Nil, "categoryName")) should equal(
      c.copy(
        dimensions = Attribute("category", Some(ConceptAttribute("p" :: Nil, "categoryName")), Nil) ::
          Attribute("date", Some(ConceptAttribute("p" :: Nil, "date")), Nil) :: Nil)
    )
    c.replaceSubExpression(ConceptReference("prices"), ConceptReference("prices1")) should equal(
      c.copy(parentConcepts = ParentConcept(ConceptReference("prices1"), Some("p"), Map(), Nil) :: Nil)
    )
    c.replaceSubExpression(StringLiteral("ca"), StringLiteral("us")) should equal(
      c.copy(attributeDependencies = Some(Equals(ConceptAttribute("p" :: Nil, "country"), StringLiteral("us"))))
    )
    c.replaceSubExpression(FloatLiteral(10), FloatLiteral(20)) should equal(
      c.copy(groupDependencies = Some(GreaterThan(ConceptAttribute(Nil, "priceAvg"), FloatLiteral(20))))
    )
    c.replaceSubExpression(ConceptAttribute(Nil, "category"), ConceptAttribute(Nil, "categoryName")) should equal(
      c.copy(orderByAttributes = Order(ConceptAttribute(Nil, "categoryName"), Order.DESC) :: Nil)
    )
    c.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      c.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "CubeInheritedConcept" should "work with subexpressions correctly" in {
    val c = CubeInheritedConcept(
      "anotherCube",
      ParentConcept(ConceptReference("someCube"), Some("c"), Map(), Nil),
      Attribute("maxVal", Some(FunctionCall(FunctionReference("max"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "avgVal") :: Nil,
      Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "date")), Nil) :: Nil,
      ConceptAttribute("c" :: Nil, "category") :: Nil,
      Some(Equals(ConceptAttribute("c" :: "c" :: Nil, "country"), StringLiteral("CA"))),
      Some(GreaterThan(ConceptAttribute("c" :: Nil, "totalVal"), IntLiteral(100))),
      Order(ConceptAttribute(Nil, "totalVal"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    c.getSubExpressions should equal(
      Equals(ConceptAttribute("c" :: "c" :: Nil, "country"), StringLiteral("CA")) ::
        GreaterThan(ConceptAttribute("c" :: Nil, "totalVal"), IntLiteral(100)) :: Nil)

    c.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    c.findSubExpression(_ == ConceptAttribute("c" :: "c" :: Nil, "val")) should equal(Some(ConceptAttribute("c" :: "c" :: Nil, "val")))
    c.findSubExpression(_ == ConceptAttribute("c" :: Nil, "avgVal")) should equal(Some(ConceptAttribute("c" :: Nil, "avgVal")))
    c.findSubExpression(_ == ConceptAttribute("c" :: "c" :: Nil, "date")) should equal(Some(ConceptAttribute("c" :: "c" :: Nil, "date")))
    c.findSubExpression(_ == ConceptAttribute("c" :: Nil, "category")) should equal(Some(ConceptAttribute("c" :: Nil, "category")))
    c.findSubExpression(_ == ConceptReference("someCube")) should equal(Some(ConceptReference("someCube")))
    c.findSubExpression(_ == StringLiteral("CA")) should equal(Some(StringLiteral("CA")))
    c.findSubExpression(_ == IntLiteral(100)) should equal(Some(IntLiteral(100)))
    c.findSubExpression(_ == ConceptAttribute(Nil, "totalVal")) should equal(Some(ConceptAttribute(Nil, "totalVal")))
    c.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    c.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    c.findAllSubExpressions(_ == ConceptAttribute("c" :: "c" :: Nil, "val")) should equal(List(ConceptAttribute("c" :: "c" :: Nil, "val")))
    c.findAllSubExpressions(_ == ConceptAttribute("c" :: Nil, "avgVal")) should equal(List(ConceptAttribute("c" :: Nil, "avgVal")))
    c.findAllSubExpressions(_ == ConceptAttribute("c" :: "c" :: Nil, "date")) should equal(List(ConceptAttribute("c" :: "c" :: Nil, "date")))
    c.findAllSubExpressions(_ == ConceptAttribute("c" :: Nil, "category")) should equal(List(ConceptAttribute("c" :: Nil, "category")))
    c.findAllSubExpressions(_ == ConceptReference("someCube")) should equal(List(ConceptReference("someCube")))
    c.findAllSubExpressions(_ == StringLiteral("CA")) should equal(List(StringLiteral("CA")))
    c.findAllSubExpressions(_ == IntLiteral(100)) should equal(List(IntLiteral(100)))
    c.findAllSubExpressions(_ == ConceptAttribute(Nil, "totalVal")) should equal(List(ConceptAttribute(Nil, "totalVal")))
    c.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    c.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(c)
    c.replaceSubExpression(FunctionReference("max"), FunctionReference("min")) should equal(
      c.copy(
        overriddenMetrics = Attribute("maxVal", Some(FunctionCall(FunctionReference("min"), ConceptAttribute("c" :: "c" :: Nil, "val") :: Nil)), Nil) :: Nil)
    )
    c.replaceSubExpression(ConceptAttribute("c" :: Nil, "avgVal"), ConceptAttribute("c" :: Nil, "averageValue")) should equal(
      c.copy(
        removedMetrics = ConceptAttribute("c" :: Nil, "averageValue") :: Nil)
    )
    c.replaceSubExpression(ConceptAttribute("c" :: "c" :: Nil, "date"), ConceptAttribute("c" :: "c" :: Nil, "orderDate")) should equal(
      c.copy(
        overriddenDimensions = Attribute("date", Some(ConceptAttribute("c" :: "c" :: Nil, "orderDate")), Nil) :: Nil)
    )
    c.replaceSubExpression(ConceptAttribute("c" :: Nil, "category"), ConceptAttribute("c" :: Nil, "categoryName")) should equal(
      c.copy(
        removedDimensions = ConceptAttribute("c" :: Nil, "categoryName") :: Nil)
    )
    c.replaceSubExpression(ConceptReference("someCube"), ConceptReference("someCubeUpdated")) should equal(
      c.copy(parentConcept = ParentConcept(ConceptReference("someCubeUpdated"), Some("c"), Map(), Nil))
    )
    c.replaceSubExpression(StringLiteral("CA"), StringLiteral("US")) should equal(
      c.copy(additionalDependencies = Some(Equals(ConceptAttribute("c" :: "c" :: Nil, "country"), StringLiteral("US"))))
    )
    c.replaceSubExpression(IntLiteral(100), FloatLiteral(100)) should equal(
      c.copy(groupDependencies = Some(GreaterThan(ConceptAttribute("c" :: Nil, "totalVal"), FloatLiteral(100))))
    )
    c.replaceSubExpression(ConceptAttribute(Nil, "totalVal"), ConceptAttribute(Nil, "totalValue")) should equal(
      c.copy(orderByAttributes = Order(ConceptAttribute(Nil, "totalValue"), Order.ASC) :: Nil)
    )
    c.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      c.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "Aggregation concept" should "work with subexpressions correctly" in {
    val ac = AggregationConcept(
      "customerOrders",
      ParentConcept(ConceptReference("customer"), Some("c"), Map(), Nil) ::
        ParentConcept(ConceptReference("order"), Some("o"), Map(), Nil) :: Nil,
      Some(Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("o" :: Nil, "customerId"))),
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    ac.getSubExpressions should equal (Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("o" :: Nil, "customerId")) :: Nil)

    ac.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    ac.findSubExpression(_ == ConceptReference("order")) should equal (Some(ConceptReference("order")))
    ac.findSubExpression(_ == ConceptAttribute("c" :: Nil, "id")) should equal (Some(ConceptAttribute("c" :: Nil, "id")))
    ac.findSubExpression(_ == StringLiteral("view")) should equal (Some(StringLiteral("view")))

    ac.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    ac.findAllSubExpressions(_ == ConceptReference("order")) should equal(List(ConceptReference("order")))
    ac.findAllSubExpressions(_ == ConceptAttribute("c" :: Nil, "id")) should equal(List(ConceptAttribute("c" :: Nil, "id")))
    ac.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    ac.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(ac)
    ac.replaceSubExpression(ConceptReference("customer"), ConceptReference("customer1")) should equal(
      ac.copy(parentConcepts = ParentConcept(ConceptReference("customer1"), Some("c"), Map(), Nil) ::
        ParentConcept(ConceptReference("order"), Some("o"), Map(), Nil) :: Nil)
    )
    ac.replaceSubExpression(ConceptAttribute("o" :: Nil, "customerId"), ConceptAttribute("o1" :: Nil, "customerId")) should equal(
      ac.copy(attributeDependencies = Some(Equals(ConceptAttribute("c" :: Nil, "id"), ConceptAttribute("o1" :: Nil, "customerId"))))
    )
    ac.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      ac.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "DataSource concept" should "work with subexpressions correctly" in {
    val dc = DataSourceConcept(
      "price",
      Attribute("price", None, Nil) :: Attribute("category", None, Annotation("Column", Map("name" -> StringLiteral("category_name"))) :: Nil) :: Nil,
      FileDataSource("path", FileFormats.CSV),
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    dc.getSubExpressions should equal (Nil)
    dc.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    dc.findSubExpression(_ == StringLiteral("category_name")) should equal (Some(StringLiteral("category_name")))
    dc.findSubExpression(_ == StringLiteral("view")) should equal (Some(StringLiteral("view")))

    dc.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(dc)
    dc.replaceSubExpression(StringLiteral("category_name"), StringLiteral("categoryName")) should equal (
      dc.copy(attributes = Attribute("price", None, Nil) :: Attribute("category", None, Annotation("Column", Map("name" -> StringLiteral("categoryName"))) :: Nil) :: Nil)
    )
    dc.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      dc.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "Function concept" should "work with subexpressions correctly" in {
    val fc = FunctionConcept(
      "arithmeticProgression",
      Attribute("value", None, Nil) :: Attribute("step", None, Annotation("Default", Map("value" -> IntLiteral(1))) :: Nil) :: Nil,
      ParentConcept(ConceptReference("IntegerNumber"), None, Map(), Nil) :: Nil,
      FunctionReference("arithmeticProgression"),
      Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil
    )

    fc.getSubExpressions should equal (FunctionReference("arithmeticProgression") :: Nil)

    fc.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    fc.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    fc.findSubExpression(_ == ConceptReference("IntegerNumber")) should equal(Some(ConceptReference("IntegerNumber")))
    fc.findSubExpression(_ == StringLiteral("ephemeral")) should equal(Some(StringLiteral("ephemeral")))

    fc.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    fc.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    fc.findAllSubExpressions(_ == ConceptReference("IntegerNumber")) should equal(List(ConceptReference("IntegerNumber")))
    fc.findAllSubExpressions(_ == StringLiteral("ephemeral")) should equal(List(StringLiteral("ephemeral")))

    fc.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(fc)
    fc.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      fc.copy(attributes = Attribute("value", None, Nil) :: Attribute("step", None, Annotation("Default", Map("value" -> IntLiteral(2))) :: Nil) :: Nil)
    )
    fc.replaceSubExpression(ConceptReference("IntegerNumber"), ConceptReference("FloatNumber")) should equal(
      fc.copy(parentConcepts = ParentConcept(ConceptReference("FloatNumber"), None, Map(), Nil) :: Nil)
    )
    fc.replaceSubExpression(StringLiteral("ephemeral"), StringLiteral("udf")) should equal(
      fc.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("udf"))) :: Nil)
    )
  }

  "Inherited concept" should "work with subexpressions correctly" in {
    val ic = InheritedConcept(
      "productCa",
      Nil,
      ParentConcept(ConceptReference("product"), None, Map(), Nil) :: Nil,
      Attribute(
        "price",
        Some(Multiply(
          ConceptAttribute(Nil, "price"),
          FunctionCall(
            FunctionReference("getCurrencyRate"),
            StringLiteral("cad") :: StringLiteral("usd") :: Nil
          )
        ))
        , Nil) :: Nil,
      ConceptAttribute(Nil, "hst") :: Nil,
      Some(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca"))),
      Order(ConceptAttribute(Nil, "name"), Order.ASC) :: Nil,
      None,
      None,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )

    ic.getSubExpressions should equal(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")) :: Nil)

    ic.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    ic.findSubExpression(_ == ConceptReference("product")) should equal(Some(ConceptReference("product")))
    ic.findSubExpression(_ == ConceptAttribute(Nil, "price")) should equal(Some(ConceptAttribute(Nil, "price")))
    ic.findSubExpression(_ == ConceptAttribute(Nil, "hst")) should equal(Some(ConceptAttribute(Nil, "hst")))
    ic.findSubExpression(_ == ConceptAttribute(Nil, "country")) should equal(Some(ConceptAttribute(Nil, "country")))
    ic.findSubExpression(_ == ConceptAttribute(Nil, "name")) should equal(Some(ConceptAttribute(Nil, "name")))
    ic.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    ic.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    ic.findAllSubExpressions(_ == ConceptReference("product")) should equal(List(ConceptReference("product")))
    ic.findAllSubExpressions(_ == ConceptAttribute(Nil, "price")) should equal(List(ConceptAttribute(Nil, "price")))
    ic.findAllSubExpressions(_ == ConceptAttribute(Nil, "hst")) should equal(List(ConceptAttribute(Nil, "hst")))
    ic.findAllSubExpressions(_ == ConceptAttribute(Nil, "country")) should equal(List(ConceptAttribute(Nil, "country")))
    ic.findAllSubExpressions(_ == ConceptAttribute(Nil, "name")) should equal(List(ConceptAttribute(Nil, "name")))
    ic.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    ic.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(ic)
    ic.replaceSubExpression(ConceptReference("product"), ConceptReference("productAll")) should equal(
      ic.copy(parentConcepts = ParentConcept(ConceptReference("productAll"), None, Map(), Nil) :: Nil)
    )
    ic.replaceSubExpression(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "priceUSD")) should equal(
      ic.copy(overriddenAttributes = Attribute(
        "price",
        Some(Multiply(
          ConceptAttribute(Nil, "priceUSD"),
          FunctionCall(
            FunctionReference("getCurrencyRate"),
            StringLiteral("cad") :: StringLiteral("usd") :: Nil
          )
        ))
        , Nil) :: Nil)
    )
    ic.replaceSubExpression(ConceptAttribute(Nil, "hst"), ConceptAttribute(Nil, "tax")) should equal(
      ic.copy(removedAttributes = ConceptAttribute(Nil, "tax") :: Nil)
    )
    ic.replaceSubExpression(ConceptAttribute(Nil, "country"), ConceptAttribute(Nil, "countryCode")) should equal(
      ic.copy(additionalDependencies = Some(Equals(ConceptAttribute(Nil, "countryCode"), StringLiteral("ca"))))
    )
    ic.replaceSubExpression(ConceptAttribute(Nil, "name"), ConceptAttribute(Nil, "price")) should equal(
      ic.copy(orderByAttributes = Order(ConceptAttribute(Nil, "price"), Order.ASC) :: Nil)
    )
    ic.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      ic.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "Set concepts" should "work with subexpressions correctly" in {
    val ic = IntersectConcept(
      "product",
      ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV2"), None, Map(), Nil)  :: Nil,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )
    ic.getSubExpressions should equal(Nil)

    ic.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    ic.findSubExpression(_ == ConceptReference("productV1")) should equal(Some(ConceptReference("productV1")))
    ic.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    ic.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    ic.findAllSubExpressions(_ == ConceptReference("productV1")) should equal(List(ConceptReference("productV1")))
    ic.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    ic.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(ic)
    ic.replaceSubExpression(ConceptReference("productV2"), ConceptReference("productV3")) should equal(
      ic.copy(parentConcepts = ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV3"), None, Map(), Nil)  :: Nil)
    )
    ic.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      ic.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )

    val mc = MinusConcept(
      "product",
      ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV2"), None, Map(), Nil) :: Nil,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )
    mc.getSubExpressions should equal(Nil)

    mc.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    mc.findSubExpression(_ == ConceptReference("productV1")) should equal(Some(ConceptReference("productV1")))
    mc.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    mc.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    mc.findAllSubExpressions(_ == ConceptReference("productV1")) should equal(List(ConceptReference("productV1")))
    mc.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    mc.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(mc)
    mc.replaceSubExpression(ConceptReference("productV2"), ConceptReference("productV3")) should equal(
      mc.copy(parentConcepts = ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV3"), None, Map(), Nil) :: Nil)
    )
    mc.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      mc.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )

    val uc = UnionConcept(
      "product",
      ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV2"), None, Map(), Nil) :: Nil,
      Annotation("Materialized", Map("type" -> StringLiteral("view"))) :: Nil
    )
    uc.getSubExpressions should equal(Nil)

    uc.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    uc.findSubExpression(_ == ConceptReference("productV1")) should equal(Some(ConceptReference("productV1")))
    uc.findSubExpression(_ == StringLiteral("view")) should equal(Some(StringLiteral("view")))

    uc.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    uc.findAllSubExpressions(_ == ConceptReference("productV1")) should equal(List(ConceptReference("productV1")))
    uc.findAllSubExpressions(_ == StringLiteral("view")) should equal(List(StringLiteral("view")))

    uc.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(uc)
    uc.replaceSubExpression(ConceptReference("productV2"), ConceptReference("productV3")) should equal(
      uc.copy(parentConcepts = ParentConcept(ConceptReference("productV1"), None, Map(), Nil) :: ParentConcept(ConceptReference("productV3"), None, Map(), Nil) :: Nil)
    )
    uc.replaceSubExpression(StringLiteral("view"), StringLiteral("ephemeral")) should equal(
      uc.copy(annotations = Annotation("Materialized", Map("type" -> StringLiteral("ephemeral"))) :: Nil)
    )
  }

  "ConceptAttributes concept" should "work with subexpressions correctly" in {
    val ca = ConceptAttributes(
      "attrList",
      Nil,
      Attribute("category", Some(ConceptAttribute("s" :: Nil, "category")), Nil) ::
        Attribute("country", Some(ConceptAttribute("s" :: Nil, "country")), Nil) :: Nil,
      ParentConcept(ConceptReference("source"), Some("s"), Map(), Nil) :: Nil,
      Annotation("Description", Map("text" -> StringLiteral("Group by columns"))) :: Nil
    )

    ca.getSubExpressions should equal (Nil)

    ca.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    ca.findSubExpression(_ == ConceptAttribute("s" :: Nil, "category")) should equal(Some(ConceptAttribute("s" :: Nil, "category")))
    ca.findSubExpression(_ == ConceptReference("source")) should equal(Some(ConceptReference("source")))
    ca.findSubExpression(_ == StringLiteral("Group by columns")) should equal(Some(StringLiteral("Group by columns")))

    ca.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    ca.findAllSubExpressions(_ == ConceptAttribute("s" :: Nil, "category")) should equal(List(ConceptAttribute("s" :: Nil, "category")))
    ca.findAllSubExpressions(_ == ConceptReference("source")) should equal(List(ConceptReference("source")))
    ca.findAllSubExpressions(_ == StringLiteral("Group by columns")) should equal(List(StringLiteral("Group by columns")))

    ca.replaceSubExpression(BooleanLiteral(false), StringLiteral("value")) should equal(ca)
    ca.replaceSubExpression(ConceptAttribute("s" :: Nil, "category"), ConceptAttribute("p" :: Nil, "category")) should equal(
      ca.copy(
        attributes = Attribute("category", Some(ConceptAttribute("p" :: Nil, "category")), Nil) ::
          Attribute("country", Some(ConceptAttribute("s" :: Nil, "country")), Nil) :: Nil)
    )
    ca.replaceSubExpression(ConceptReference("source"), ConceptReference("source1")) should equal(
      ca.copy(parentConcepts = ParentConcept(ConceptReference("source1"), Some("s"), Map(), Nil) :: Nil)
    )
    ca.replaceSubExpression(StringLiteral("Group by columns"), StringLiteral("Dimension keys")) should equal(
      ca.copy(annotations = Annotation("Description", Map("text" -> StringLiteral("Dimension keys"))) :: Nil)
    )
  }




}
