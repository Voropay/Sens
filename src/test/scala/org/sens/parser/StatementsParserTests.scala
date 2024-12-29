package org.sens.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression
import org.sens.core.expression.{CollectionItem, FunctionCall, NamedElementPlaceholder, Variable}
import org.sens.core.expression.function._
import org.sens.core.expression.literal.{BooleanLiteral, IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.{Add, Multiply}
import org.sens.core.expression.operation.comparison._
import org.sens.core.statement._

class StatementsParserTests extends AnyFlatSpec with Matchers {

  val statementsParser = new SensParser {
    def apply(code: String): Option[SensStatement] = {
      parse(sensStatement, code) match {
        case Success(res, _) => Some(res)
        case _ => None
      }
    }
  }

  "Variable statements" should "be parsed correctly" in {
    statementsParser("var a").get should equal (VariableDefinition("a", None))
    statementsParser("var a = 1").get should equal (VariableDefinition("a", Some(IntLiteral(1))))
    statementsParser("a = 1").get should equal (VariableAssignment("a", IntLiteral(1)))
    statementsParser("a++").get should equal (Increment(Variable("a")))
    statementsParser("a--").get should equal (Decrement(Variable("a")))
  }

  "Control statements" should "be parsed correctly" in {
    statementsParser("return a").get should equal (Return(Some(NamedElementPlaceholder("a"))))
    statementsParser("return").get should equal (Return(None))
    statementsParser("break").get should equal (Break())
    statementsParser("continue").get should equal (Continue())
  }

  "Add to collection statements" should "be parsed correctly" in {
    statementsParser("a[] = 1").get should equal (AddToList(Variable("a"), IntLiteral(1)))
    statementsParser("a[] = b[i]").get should equal (AddToList(Variable("a"), CollectionItem(NamedElementPlaceholder("b"), NamedElementPlaceholder("i") :: Nil)))

    statementsParser("a[i] = 1").get should equal (AddToMap(Variable("a"), NamedElementPlaceholder("i"), IntLiteral(1)))
    statementsParser("a[i] = b[i]").get should equal (AddToMap(Variable("a"), NamedElementPlaceholder("i"), CollectionItem(NamedElementPlaceholder("b"), NamedElementPlaceholder("i") :: Nil)))
  }

  "Function statements" should "be parsed correctly" in {
    statementsParser("a(1, b, false)").get should equal (ProcedureCall(
      FunctionCall(
        FunctionReference("a"),
        IntLiteral(1) :: NamedElementPlaceholder("b") :: BooleanLiteral(false) :: Nil
      )
    ))
    statementsParser("var a = random()").get should equal (VariableDefinition("a", Some(
      expression.FunctionCall(FunctionReference("random"), Nil)
    )))

    statementsParser("def a(a, b, c) return a + b +c").get should equal (
      FunctionDefinition(
        "a",
        "a" :: "b" :: "c" :: Nil,
        Return(Some(Add(
          NamedElementPlaceholder("a"), Add(NamedElementPlaceholder("b"), NamedElementPlaceholder("c"))
        )))
      )
    )
  }

  "If statement" should "be parsed correctly" in {
    statementsParser("if a = b then return").get should equal (If(
      Equals(NamedElementPlaceholder("a"), NamedElementPlaceholder("b")),
      Return(None),
      None
    ))
    statementsParser("if a > b then return a else return b").get should equal (If(
      GreaterThan(NamedElementPlaceholder("a"), NamedElementPlaceholder("b")),
      Return(Some(NamedElementPlaceholder("a"))),
      Some(Return(Some(NamedElementPlaceholder("b"))))
    ))
    statementsParser("if a = 1 then return \"one\" else if a = 2 then return \"two\" else return \"?\"").get should equal (
      If(
        Equals(NamedElementPlaceholder("a"), IntLiteral(1)),
        Return(Some(StringLiteral("one"))),
        Some(If(
          Equals(NamedElementPlaceholder("a"), IntLiteral(2)),
          Return(Some(StringLiteral("two"))),
          Some(Return(Some(StringLiteral("?"))))
        ))
    ))
  }

  "For statement" should "be parsed correctly" in {
    statementsParser("for(var i = 0; i < 10; i++) println(i)").get should equal (
      For(
        Some(VariableDefinition("i", Some(IntLiteral(0)))),
        Some(LessThan(NamedElementPlaceholder("i"), IntLiteral(10))),
        Some(Increment(Variable("i"))),
        ProcedureCall(expression.FunctionCall(FunctionReference("println"), NamedElementPlaceholder("i") :: Nil))
      )
    )
    statementsParser("for(; ; ) doSomething()").get should equal (
      For(None, None, None, ProcedureCall(expression.FunctionCall(FunctionReference("doSomething"), Nil)))
    )

    statementsParser("for(item in list) println(item)").get should equal (
      ForEachItemInList(
        "item",
        NamedElementPlaceholder("list"),
        ProcedureCall(expression.FunctionCall(FunctionReference("println"), NamedElementPlaceholder("item") :: Nil))
      )
    )

    statementsParser("for(key, value in map) println(key + \": \" + value)").get should equal (
      ForEachItemInMap(
        "key",
        "value",
        NamedElementPlaceholder("map"),
        ProcedureCall(expression.FunctionCall(
          FunctionReference("println"),
          Add(NamedElementPlaceholder("key"), Add(StringLiteral(": "), NamedElementPlaceholder("value"))) :: Nil
        ))
      )
    )

    statementsParser("while i < 10 do i++").get should equal (
      While(
        LessThan(NamedElementPlaceholder("i"), IntLiteral(10)),
        Increment(Variable("i"))
      )
    )

    statementsParser("do i++ while i < 10").get should equal (
      DoWhile(
        LessThan(NamedElementPlaceholder("i"), IntLiteral(10)),
        Increment(Variable("i"))
      )
    )
  }

  "Statement block" should "be parsed correctly" in {
    statementsParser("{var i = 0; i = i + 1; return i}").get should equal (
      StatementBlock(
        VariableDefinition("i", Some(IntLiteral(0))) ::
        VariableAssignment("i", Add(NamedElementPlaceholder("i"), IntLiteral(1))) ::
        Return(Some(NamedElementPlaceholder("i"))) :: Nil
      )
    )

    statementsParser(
      "{\n" +
        "def pow(v, n) {\n" +
        "    var s = 1;\n" +
        "    for(var i = 0; i < n; i++) {\n" +
        "        s = s * v;\n" +
        "    };\n" +
        "    return s\n" +
        "};\n" +
        "println(pow(2, 10))\n" +
      "}"
    ).get should equal (
      StatementBlock(
        FunctionDefinition(
          "pow",
          "v" :: "n" :: Nil,
          StatementBlock(
            VariableDefinition("s", Some(IntLiteral(1))) ::
              For(
                Some(VariableDefinition("i", Some(IntLiteral(0)))),
                Some(LessThan(NamedElementPlaceholder("i"), NamedElementPlaceholder("n"))),
                Some(Increment(Variable("i"))),
                StatementBlock(VariableAssignment("s", Multiply(NamedElementPlaceholder("s"), NamedElementPlaceholder("v"))) :: Nil)
              ) ::
              Return(Some(NamedElementPlaceholder("s"))) :: Nil
          )
        ) ::
          ProcedureCall(expression.FunctionCall(
            FunctionReference("println"),
            expression.FunctionCall(FunctionReference("pow"), IntLiteral(2) :: IntLiteral(10) :: Nil) :: Nil
          )) :: Nil
      )
    )
  }
}
