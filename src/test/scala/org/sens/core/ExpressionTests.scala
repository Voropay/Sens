package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import org.sens.core.concept._
import org.sens.core.expression.concept._
import org.sens.core.expression.function._
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.core.expression._
import org.sens.core.statement.{ConceptDefinition, InvisibleReturn, Return}
import org.sens.parser.ValidationContext

class ExpressionTests extends AnyFlatSpec with Matchers {

  "Literals" should "be formatted in Sens correctly" in {
    BooleanLiteral(true).toSensString should equal ("true")
    BooleanLiteral(false).toSensString should equal ("false")

    FloatLiteral(1).toSensString should equal ("1.0")
    FloatLiteral(-1).toSensString should equal ("-1.0")
    FloatLiteral(1.2).toSensString should equal ("1.2")

    IntLiteral(0).toSensString should equal ("0")
    IntLiteral(-1).toSensString should equal ("-1")
    IntLiteral(1).toSensString should equal ("1")

    StringLiteral("test").toSensString should equal ("\"test\"")

    NullLiteral().toSensString should equal ("Null")

    IntTypeLiteral().toSensString should equal ("int")
    FloatTypeLiteral().toSensString should equal ("float")
    StringTypeLiteral(255).toSensString should equal ("string(255)")
    BooleanTypeLiteral().toSensString should equal ("boolean")
    ListTypeLiteral(IntTypeLiteral()).toSensString should equal ("list<int>")
    MapTypeLiteral(Map("attr1" -> IntTypeLiteral(), "attr2" -> BooleanTypeLiteral())).toSensString should equal ("map<int attr1,\nboolean attr2>")
    MapTypeLiteral(
      Map("attr1" -> ListTypeLiteral(IntTypeLiteral()), "attr2" -> BooleanTypeLiteral())
    ).toSensString should equal ("map<list<int> attr1,\nboolean attr2>")

    ListLiteral(IntLiteral(1) :: StringLiteral("plus") :: FloatLiteral(1.2) :: Nil).toSensString should equal ("[1, \"plus\", 1.2]")

    val mapStr = "[\"key1\": \"strValue\",\n\"key2\": true]"
    MapLiteral(Map(StringLiteral("key1") -> StringLiteral("strValue"), StringLiteral("key2") -> BooleanLiteral(true))).toSensString should equal (mapStr)
  }

  "Named object expressions" should "be formatted in Sens correctly" in {
    Variable("a").toSensString should equal ("a")

    NamedElementPlaceholder("a").toSensString should equal ("a")
    GenericParameter("a").toSensString should equal ("a")

    If(Variable("a"), IntLiteral(1), IntLiteral(-1)).toSensString should equal ("(if a then 1 else -1)")

    val conditions: Map[SensExpression, SensExpression] = Map(
      Equals(Variable("a"), IntLiteral(1)) -> StringLiteral("one"),
      Equals(Variable("a"), IntLiteral(2)) -> StringLiteral("two"),
      Equals(Variable("a"), IntLiteral(3)) -> StringLiteral("three")
    )
    val switchStr = "(case when (a = 1) then \"one\" when (a = 2) then \"two\" when (a = 3) then \"three\" else \"?\")"
    Switch(conditions, StringLiteral("?")).toSensString should equal (switchStr)

    val methodInvocationExpr = MethodInvocation("a" :: "func" :: Nil, IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
    methodInvocationExpr.toSensString should equal ("a.func(1, 2, 3)")

    ConceptAttribute("a" :: "b" :: Nil, "attr").toSensString should equal ("a.b.attr")
    ConceptAttribute(Nil, "attr").toSensString should equal ("attr")
    ConceptAttribute(List(), "attr").toSensString should equal ("attr")

    ConceptObject("c1").toSensString should equal ("c1")

    val mapInitExpr = MapInitialization(Map(StringLiteral("a") -> IntLiteral(1), StringLiteral("b") -> IntLiteral(2)))
    mapInitExpr.toSensString should equal ("[\"a\": 1, \"b\": 2]")

    val listInitExpr = ListInitialization(IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
    listInitExpr.toSensString should equal ("[1, 2, 3]")

    val collectionAttrExpr = CollectionItem(Variable("a"), Variable("b") :: IntLiteral(1) :: StringLiteral("attr") :: Nil)
    collectionAttrExpr.toSensString should equal ("a[b][1][\"attr\"]")
  }

  "Collection Item expression" should "be transformed to nested collection item chain correctly" in {
    val collectionAttrExpr = CollectionItem(Variable("a"), Variable("b") :: IntLiteral(1) :: StringLiteral("attr") :: Nil)
    val collectionAttrExprChain = collectionAttrExpr.toCollectionItemChain
    collectionAttrExprChain should equal (
      CollectionItem(
        CollectionItem(
          CollectionItem(
            Variable("a"),
            Variable("b") :: Nil
          ),
          IntLiteral(1) :: Nil
        ),
        StringLiteral("attr") :: Nil
      )
    )
  }

  "Operations" should "be formatted in Sens correctly" in {
    val arithmeticExpr = Divide(
      Multiply(
        Add(Variable("a"), Variable("b")),
        Substract(Variable("a"), Variable("b"))
      ),
      IntLiteral(2)
    )
    arithmeticExpr.toSensString should equal ("(((a + b) * (a - b)) / 2)")

    val comparisonExpr = And(
      Equals(Variable("a"), Variable("b")),
      And(
        NotEquals(Variable("c"), Variable("d")),
        And(
          GreaterThan(Variable("e"), Variable("f")),
          And(
            GreaterOrEqualsThan(Variable("g"), Variable("h")),
            And(
              LessThan(Variable("i"), Variable("j")),
              LessOrEqualsThan(Variable("k"), Variable("l"))
            )
          )
        )
      )
    )
    comparisonExpr.toSensString should equal ("((a = b) and ((c != d) and ((e > f) and ((g >= h) and ((i < j) and (k <= l))))))")

    val betweenExpr = Between(Variable("a"), Variable("b"), Variable("c"))
    betweenExpr.toSensString should equal ("a between [b, c]")

    val logicalExpr = Or(
      And(Variable("a"), Variable("b")),
      Not(
        And(Variable("a"), Variable("b"))
      )
    )
    logicalExpr.toSensString should equal ("((a and b) or (not (a and b)))")

    val andSeqExpr = AndSeq(
      Variable("a") :: Variable("b") :: Variable("c") :: Nil
    )
    andSeqExpr.toSensString should equal ("(a and b and c)")
  }

  "Functions" should "be formatted in Sens correctly" in {
    FunctionReference("func").toSensString should equal ("func")

    expression.FunctionCall(FunctionReference("func"), IntLiteral(1) :: IntLiteral(2) :: Nil).toSensString should equal ("func(1, 2)")

    val funcDefExpr = AnonymousFunctionDefinition(
      "a" :: "b" :: "c" :: Nil,
      Return(Some(Add(Variable("a"), Add(Variable("b"), Variable("c")))))
    )
    funcDefExpr.toSensString should equal ("(a, b, c) => return (a + (b + c))")

    val anonFuncCall = expression.FunctionCall(funcDefExpr, IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
    anonFuncCall.toSensString should equal ("((a, b, c) => return (a + (b + c)))(1, 2, 3)")
  }

  "Concept reference" should "correctly return name and attributes" in {
    val context = ValidationContext()
    context.addConcept(
      ConceptDefinition(Concept.builder(
        "concept1",
        Attribute("id", None, Nil) :: Attribute("val", None, Annotation.OPTIONAL :: Nil) :: Attribute("timeCreated", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))
    val ref = ConceptReference("concept1")
    ref.getName should equal ("concept1")
    ref.getAttributeNames(context) should equal (List("id", "val", "timeCreated"))
    ref.getAttributes(context) should equal (
      Attribute("id", None, Nil) ::
        Attribute("val", None, Annotation.OPTIONAL :: Nil) ::
        Attribute("timeCreated", None, Nil) :: Nil
    )
  }

  "Anonymous concept" should "correctly return attributes" in {
    val context = ValidationContext()
    context.addConcept(
      ConceptDefinition(Concept.builder(
        "concept1",
        Attribute("id", None, Nil) :: Attribute("val", None, Nil) :: Attribute("timeCreated", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))

    context.addConcept(
      ConceptDefinition(Concept.builder(
        "concept2",
        Attribute("id", None, Nil) ::
          Attribute("val", None, Annotation.OPTIONAL :: Nil) ::
          Attribute("concept1id", None, Annotation.OPTIONAL :: Nil) ::
          Attribute("timeCreated", None, Nil) :: Nil,
        ParentConcept(ConceptReference("someConcept"), Some("c1"), Map(), Nil) :: Nil)
        .build
      ))

    val anonConDef1 = AnonymousConceptDefinition.builder(
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Annotation.OPTIONAL :: Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")))
      .build

    anonConDef1.getAttributeNames(context) should equal (List("attr1"))
    anonConDef1.getAttributes(context) should equal (Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Annotation.OPTIONAL :: Nil) :: Nil)

    val anonConDef2 = AnonymousConceptDefinition.builder(
      Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")))
      .build

    anonConDef2.getAttributeNames(context) should equal (List("id", "val", "timeCreated", "concept1id"))
    anonConDef2.getAttributes(context) should equal (
      Attribute("id", None, Nil) ::
      Attribute("val", None, Nil) ::
      Attribute("timeCreated", None, Nil) ::
      Attribute("concept1id", None, Annotation.OPTIONAL :: Nil) :: Nil
    )
  }

  "Concepts" should "be formatted in Sens correctly" in {
    ConceptReference("c").toSensString should equal ("c")

    val anonConDef1 = AnonymousConceptDefinition(
      Attribute("attr1", None, Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil,
      Some(And(
        Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")),
        Equals(ConceptAttribute("c1" :: Nil, "val"), ConceptAttribute(Nil, "attr1"))
      )),
      ConceptAttribute(Nil, "attr1") :: Nil,
      Some(GreaterThan(ConceptAttribute(Nil, "attr1"), IntLiteral(0))),
      Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil,
      Some(10),
      Some(100),
      Annotation.UNIQUE :: Nil
    )
    val anonConDef1Str = "< @Unique (attr1) from concept1 c1, concept2 c2 where ((c1.id = c2.concept1id) and (c1.val = attr1)) group by attr1 having (attr1 > 0) order by attr1 ASC limit 10 offset 100 >"
    anonConDef1.toSensString should equal(anonConDef1Str)

    val anonConDef2 = AnonymousConceptDefinition.builder(
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")))
      .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
      .orderByAttributes(Order(ConceptAttribute(Nil, "attr1"), Order.ASC) :: Nil)
      .limit(10)
      .build

    val anonConDef2Str = "< (attr1 = c1.val) from concept1 c1, concept2 c2 where (c1.id = c2.concept1id) group by attr1 order by attr1 ASC limit 10 >"
    anonConDef2.toSensString should equal(anonConDef2Str)

    val anonConDef3 = AnonymousConceptDefinition.builder(
      Attribute("attr1", Some(ConceptAttribute("c1" :: Nil, "val")), Nil) :: Nil,
      ParentConcept(ConceptReference("concept1"), Some("c1"), Map(), Nil) :: ParentConcept(ConceptReference("concept2"), Some("c2"), Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "concept1id")))
      .groupByAttributes(ConceptAttribute(Nil, "attr1") :: Nil)
      .build

    val anonConDef3Str = "< (attr1 = c1.val) from concept1 c1, concept2 c2 where (c1.id = c2.concept1id) group by attr1 >"
    anonConDef3.toSensString should equal(anonConDef3Str)

    val anonConDef4 = AnonymousConceptDefinition.builder(
      Nil,
      ParentConcept(ConceptReference("concept1"), None, Map(), Nil) :: Nil)
      .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), IntLiteral(1)))
      .build

    val anonConDef4Str = "< concept1 where (c1.id = 1) >"
    anonConDef4.toSensString should equal(anonConDef4Str)

    val anonFuncConDef = AnonymousFunctionConceptDefinition(
      InvisibleReturn(Some(ConceptAttribute("c1" :: Nil, "items")))
    )
    anonFuncConDef.toSensString should equal ("< c1.items >")
  }

  "Relational operations" should "be formatted in Sens correctly" in {
    InList(
      ConceptAttribute("c1" :: Nil, "val"),
      ListInitialization(
        List(IntLiteral(1), IntLiteral(2), IntLiteral(3))
      )).toSensString should equal ("(c1.val in [1, 2, 3])")

    InSubQuery(
      ConceptAttribute("c1" :: Nil, "city"),
      AnonymousConceptDefinition.builder(
        Attribute("city", None, Nil) :: Nil,
        ParentConcept(ConceptReference("location"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "country"), ConceptAttribute("c2" :: Nil, "country")))
        .build
    ).toSensString should equal ("(c1.city in < (city) from location c2 where (c1.country = c2.country) >)")

    All(
      GreaterThan(
        ConceptAttribute("c1" :: Nil, "price"),
        AnonymousConceptDefinition.builder(
          Attribute("price", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
          .build
      )
    ).toSensString should equal ("(c1.price > all < (price) from products c2 where (c2.category = 1) >)")

    Any(
      GreaterOrEqualsThan(
        ConceptAttribute("c1" :: Nil, "price"),
        AnonymousConceptDefinition.builder(
          Attribute("price", None, Nil) :: Nil,
          ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
          .build
      )
    ).toSensString should equal("(c1.price >= any < (price) from products c2 where (c2.category = 1) >)")

    Exists(
      AnonymousConceptDefinition.builder(
        Attribute("id", None, Nil) :: Nil,
        ParentConcept(ConceptReference("orderItem"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "productId")))
        .build
    ).toSensString should equal("exists < (id) from orderItem c2 where (c1.id = c2.productId) >")

    Unique(
      AnonymousConceptDefinition.builder(
        Attribute("email", None, Nil) :: Nil,
        ParentConcept(ConceptReference("customer"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "email"), ConceptAttribute("c2" :: Nil, "email")))
        .build
    ).toSensString should equal("unique < (email) from customer c2 where (c1.email = c2.email) >")

  }

  "Window functions" should "be formatted in Sens correctly" in {
    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      ConceptAttribute(Nil, "person") :: Nil,
      Order(ConceptAttribute(Nil, "date"), Order.DESC) :: Nil,
      (None, None),
      (Some(IntLiteral(1)), Some(IntLiteral(10)))
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "partition by (person)\n" +
      "order by (date DESC)\n" +
      "range between (1, 10)\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      ConceptAttribute(Nil, "person") :: Nil,
      Order(ConceptAttribute(Nil, "date"), Order.DESC) :: Nil,
      (Some(IntLiteral(1)), Some(IntLiteral(10))),
      (None, None)
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "partition by (person)\n" +
      "order by (date DESC)\n" +
      "rows between (1, 10)\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      Nil,
      Order(ConceptAttribute(Nil, "date"), Order.DESC) :: Nil,
      (None, Some(IntLiteral(10))),
      (None, None)
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "order by (date DESC)\n" +
      "rows between (unbounded, 10)\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      ConceptAttribute(Nil, "person") :: Nil,
      Nil,
      (Some(IntLiteral(1)), None),
      (None, None)
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "partition by (person)\n" +
      "rows between (1, unbounded)\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      Nil,
      Nil,
      (None, None),
      (None, None)
    ).toSensString should equal("sum((price * amount)) over (\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      Nil,
      Order(ConceptAttribute(Nil, "date"), Order.DESC) :: Nil,
      (None, None),
      (None, Some(IntLiteral(10)))
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "order by (date DESC)\n" +
      "range between (unbounded, 10)\n" +
      ")")

    WindowFunction(
      WindowFunctions.SUM,
      Multiply(ConceptAttribute(Nil, "price"), ConceptAttribute(Nil, "amount")) :: Nil,
      Nil,
      Order(ConceptAttribute(Nil, "date"), Order.DESC) :: Nil,
      (None, None),
      (Some(IntLiteral(1)), None)
    ).toSensString should equal("sum((price * amount)) over (\n" +
      "order by (date DESC)\n" +
      "range between (1, unbounded)\n" +
      ")")
  }
}
