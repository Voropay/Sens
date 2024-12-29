package org.sens.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression
import org.sens.core.expression.concept.AnonymousConceptDefinition
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference}
import org.sens.core.expression._
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.core.statement.{Return, StatementBlock}
import org.sens.core.concept._
import org.sens.core.expression.concept._

class ExpressionsParserTests extends AnyFlatSpec with Matchers {

  val expressionsParser = new SensParser {
    def apply(code: String): Option[SensExpression] = {
      parse(sensExpression, code) match {
        case Success(res, _) => Some(res)
        case _ => None
      }
    }
  }

  "Variables" should "be parsed correctly" in {
    expressionsParser("Nulla").get should equal (NamedElementPlaceholder("Nulla"))
    expressionsParser("a.b").get should equal (ConceptAttribute("a" :: Nil, "b"))
    expressionsParser("a.b.c").get should equal (ConceptAttribute("a" :: "b" :: Nil, "c"))
  }

  "Collections items" should "be parsed correctly" in {
    expressionsParser("a[0]").get should equal (CollectionItem(NamedElementPlaceholder("a"), IntLiteral(0) :: Nil))
    expressionsParser("a[b]").get should equal (CollectionItem(NamedElementPlaceholder("a"), NamedElementPlaceholder("b") :: Nil))
    expressionsParser("a[b][c]").get should equal (CollectionItem(NamedElementPlaceholder("a"), NamedElementPlaceholder("b") :: NamedElementPlaceholder("c") :: Nil))
    expressionsParser("a[b.c][d[e]]").get should equal (
      CollectionItem(NamedElementPlaceholder("a"),
        ConceptAttribute("b" :: Nil, "c") ::
          CollectionItem(NamedElementPlaceholder("d"), NamedElementPlaceholder("e") :: Nil) ::
          Nil)
    )
  }

  "Functions" should "be parsed correctly" in {
    expressionsParser("a()").get should equal (FunctionCall(FunctionReference("a"), Nil))
    expressionsParser("a(1, b, false)").get should equal (expression.FunctionCall(
      FunctionReference("a"),
      IntLiteral(1) :: NamedElementPlaceholder("b") :: BooleanLiteral(false) :: Nil
    ))
    expressionsParser("a(b(c(d)))").get should equal (expression.FunctionCall(
      FunctionReference("a"), expression.FunctionCall(
        FunctionReference("b"), expression.FunctionCall(
          FunctionReference("c"), NamedElementPlaceholder("d") :: Nil
        ) :: Nil
      ) :: Nil
    ))
    expressionsParser("(a, b) => {return a} (1, 2)").get should equal (expression.FunctionCall(
      AnonymousFunctionDefinition(
        "a" :: "b" :: Nil,
        StatementBlock(Return(Some(NamedElementPlaceholder("a"))) :: Nil)),
      IntLiteral(1) :: IntLiteral(2) :: Nil
    ))

    expressionsParser("(a) => {return a}").get should equal (
      AnonymousFunctionDefinition(
        "a" :: Nil,
        StatementBlock(Return(Some(NamedElementPlaceholder("a"))) :: Nil))
    )
  }

  "Methods calls" should "be parsed correctly" in {
    expressionsParser("a.f()").get should equal (MethodInvocation("a" :: "f" :: Nil, Nil))
    expressionsParser("a.f(1, b, false)").get should equal (
      MethodInvocation(
        "a" :: "f" :: Nil,
        IntLiteral(1) :: NamedElementPlaceholder("b") :: BooleanLiteral(false) :: Nil
      ))
  }

  "Operations" should "be parsed correctly" in {
    expressionsParser("a + b + c").get should equal (
      Add(NamedElementPlaceholder("a"), Add(NamedElementPlaceholder("b"), NamedElementPlaceholder("c")))
    )
    expressionsParser("a + b * c - d / e").get should equal (
      Add(
        NamedElementPlaceholder("a"),
        Substract(
          Multiply(NamedElementPlaceholder("b"), NamedElementPlaceholder("c")),
          Divide(NamedElementPlaceholder("d"), NamedElementPlaceholder("e"))
        )
      )
    )
    expressionsParser("a = b and c != d and e > f and g >= h and i < j and k <= l").get should equal (
      And(
        Equals(NamedElementPlaceholder("a"), NamedElementPlaceholder("b")),
        And(
          NotEquals(NamedElementPlaceholder("c"), NamedElementPlaceholder("d")),
          And(
            GreaterThan(NamedElementPlaceholder("e"), NamedElementPlaceholder("f")),
            And(
              GreaterOrEqualsThan(NamedElementPlaceholder("g"), NamedElementPlaceholder("h")),
              And(
                LessThan(NamedElementPlaceholder("i"), NamedElementPlaceholder("j")),
                LessOrEqualsThan(NamedElementPlaceholder("k"), NamedElementPlaceholder("l"))
              )
            )
          )
        )
      )
    )
    expressionsParser("a between [b, c]").get should equal (
      Between(NamedElementPlaceholder("a"), NamedElementPlaceholder("b"), NamedElementPlaceholder("c"))
    )

    expressionsParser("a and b or not c").get should equal (
      And(
        NamedElementPlaceholder("a"),
        Or(
          NamedElementPlaceholder("b"),
          Not(NamedElementPlaceholder("c"))
        )
      )
    )

    expressionsParser("(a + b) * (a - b)").get should equal (
      Multiply(
        Add(NamedElementPlaceholder("a"), NamedElementPlaceholder("b")),
        Substract(NamedElementPlaceholder("a"), NamedElementPlaceholder("b"))
      )
    )

    expressionsParser("a + - b").get should equal (
      Add(NamedElementPlaceholder("a"), UnaryMinus(NamedElementPlaceholder("b")))
    )
  }

  "List initialization" should "be parsed correctly" in {
    expressionsParser("[]").get should equal (ListInitialization(Nil))
    expressionsParser("[1]").get should equal (ListInitialization(IntLiteral(1) :: Nil))

    expressionsParser("[a, \"str\", [b, c + d]]").get should equal (
      ListInitialization(
        NamedElementPlaceholder("a") ::
          StringLiteral("str") ::
          ListInitialization(
            NamedElementPlaceholder("b") ::
              Add(NamedElementPlaceholder("c"), NamedElementPlaceholder("d")) :: Nil
          ) :: Nil
      )
    )
  }

  "Map initialization" should "be parsed correctly" in {
    expressionsParser("[1: 2]").get should equal (
      MapInitialization(Map(IntLiteral(1) -> IntLiteral(2)))
    )

    expressionsParser("[\"a\": 1, b: c]").get should equal (
      MapInitialization(Map(
        StringLiteral("a") -> IntLiteral(1),
        NamedElementPlaceholder("b") -> NamedElementPlaceholder("c")
      ))
    )

    expressionsParser("[\"a\": 1, b: c, d + 1: [e, f], func(g): [h: i + j, k: l > m]]").get should equal (
      MapInitialization(Map(
        StringLiteral("a") -> IntLiteral(1),
        NamedElementPlaceholder("b") -> NamedElementPlaceholder("c"),
        Add(
          NamedElementPlaceholder("d"),
          IntLiteral(1)) -> ListInitialization(NamedElementPlaceholder("e") :: NamedElementPlaceholder("f") :: Nil),
        expression.FunctionCall(FunctionReference("func"), NamedElementPlaceholder("g") :: Nil) -> MapInitialization(Map(
          NamedElementPlaceholder("h") -> Add(NamedElementPlaceholder("i"), NamedElementPlaceholder("j")),
          NamedElementPlaceholder("k") -> GreaterThan(NamedElementPlaceholder("l"), NamedElementPlaceholder("m"))
        ))
      ))
    )

    expressionsParser("[[a, b]: [c, d], [e: f, g: h]: [i: j, k: l]]").get should equal (
      MapInitialization(Map(
        ListInitialization(NamedElementPlaceholder("a") :: NamedElementPlaceholder("b") :: Nil) ->
          ListInitialization(NamedElementPlaceholder("c") :: NamedElementPlaceholder("d") :: Nil),
        MapInitialization(Map(
          NamedElementPlaceholder("e") -> NamedElementPlaceholder("f"),
          NamedElementPlaceholder("g") -> NamedElementPlaceholder("h")
        )) ->
          MapInitialization(Map(
            NamedElementPlaceholder("i") -> NamedElementPlaceholder("j"),
            NamedElementPlaceholder("k") -> NamedElementPlaceholder("l")
          ))
      ))
    )
  }

  "conditional expressions" should "be parsed correctly" in {
    expressionsParser("if a > 0 then a else 0").get should equal (
      If(GreaterThan(NamedElementPlaceholder("a"), IntLiteral(0)), NamedElementPlaceholder("a"), IntLiteral(0))
    )

    expressionsParser("case when a < min then -1 when a > max then 1 else 0").get should equal (
      Switch(Map(
        LessThan(NamedElementPlaceholder("a"), NamedElementPlaceholder("min")) -> IntLiteral(-1),
        GreaterThan(NamedElementPlaceholder("a"), NamedElementPlaceholder("max")) -> IntLiteral(1)
      ), IntLiteral(0)
      )
    )
  }

  "relational expressions" should "be parsed correctly" in {
    expressionsParser("a in [1, 2, 3]").get should equal(
      InList(
        NamedElementPlaceholder("a"),
        ListInitialization(IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
      )
    )

    expressionsParser("c1.city in <(city) from location c2 where (c1.country = c2.country)>").get should equal(
      InSubQuery(
      ConceptAttribute("c1" :: Nil, "city"),
      AnonymousConceptDefinition.builder(
        Attribute("city", None, Nil) :: Nil,
        ParentConcept(ConceptReference("location"), Some("c2"), Map(), Nil) :: Nil)
        .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "country"), ConceptAttribute("c2" :: Nil, "country")))
        .build
      )
    )

    expressionsParser("c1.price > all < (price) from products c2 where (c2.category = 1) >").get should equal(
      All(
        GreaterThan(
          ConceptAttribute("c1" :: Nil, "price"),
          AnonymousConceptDefinition.builder(
            Attribute("price", None, Nil) :: Nil,
            ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
            .build
        )
      )
    )

    expressionsParser("c1.price >= any < (price) from products c2 where (c2.category = 1) >").get should equal(
      Any(
        GreaterOrEqualsThan(
          ConceptAttribute("c1" :: Nil, "price"),
          AnonymousConceptDefinition.builder(
            Attribute("price", None, Nil) :: Nil,
            ParentConcept(ConceptReference("products"), Some("c2"), Map(), Nil) :: Nil)
            .attributeDependencies(Equals(ConceptAttribute("c2" :: Nil, "category"), IntLiteral(1)))
            .build
        )
      )
    )

    expressionsParser("exists < (id) from orderItem c2 where (c1.id = c2.productId) >").get should equal(
      Exists(
        AnonymousConceptDefinition.builder(
          Attribute("id", None, Nil) :: Nil,
          ParentConcept(ConceptReference("orderItem"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "id"), ConceptAttribute("c2" :: Nil, "productId")))
          .build
      )
    )

    expressionsParser("unique < (email) from customer c2 where (c1.email = c2.email) >").get should equal(
      Unique(
        AnonymousConceptDefinition.builder(
          Attribute("email", None, Nil) :: Nil,
          ParentConcept(ConceptReference("customer"), Some("c2"), Map(), Nil) :: Nil)
          .attributeDependencies(Equals(ConceptAttribute("c1" :: Nil, "email"), ConceptAttribute("c2" :: Nil, "email")))
          .build
      )
    )
  }

  "window functions" should "be parsed correctly" in {
    expressionsParser("sum(price) over ()").get should equal (
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        Nil,
        Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Nil,
        (None, None),
        (None, None)
      )
    )


    expressionsParser("sum(price) over (partition by (category, country))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: NamedElementPlaceholder("country")  :: Nil,
        Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("count() over (order by (category asc))").get should equal(
      WindowFunction(
        WindowFunctions.COUNT,
        Nil,
        Nil,
        Order(ConceptAttribute(Nil ,"category"), Order.ASC) :: Nil,
        (None, None),
        (None, None)
      )
    )


    expressionsParser("count() over (order by (category asc, country desc))").get should equal(
      WindowFunction(
        WindowFunctions.COUNT,
        Nil,
        Nil,
        Order(ConceptAttribute(Nil, "category"), Order.ASC) :: Order(ConceptAttribute(Nil, "country"), Order.DESC) :: Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) rows between (unbounded, unbounded))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) rows between (1, unbounded))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (Some(IntLiteral(1)), None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) rows between (unbounded, 10))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, Some(IntLiteral(10))),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) rows between (1, 10))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (Some(IntLiteral(1)), Some(IntLiteral(10))),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) range between (unbounded, unbounded))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (None, None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) range between (1, unbounded))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (Some(IntLiteral(1)), None)
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) range between (unbounded, 10))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (None, Some(IntLiteral(10)))
      )
    )

    expressionsParser("sum(price) over (partition by (category) order by (date asc) range between (1, 10))").get should equal(
      WindowFunction(
        WindowFunctions.SUM,
        NamedElementPlaceholder("price") :: Nil,
        NamedElementPlaceholder("category") :: Nil,
        Order(ConceptAttribute(Nil, "date"), Order.ASC) :: Nil,
        (None, None),
        (Some(IntLiteral(1)), Some(IntLiteral(10)))
      )
    )
  }

}
