package org.sens.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{InheritedConcept, ParentConcept}
import org.sens.core.expression.{FunctionCall, Variable}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.comparison.LessThan
import org.sens.core.statement._

class StatementTests extends AnyFlatSpec with Matchers {
  "Add to collection statements" should "be formatted in Sens correctly" in {
    AddToList(Variable("a"), IntLiteral(1)).toSensString should equal ("a[] = 1")
    AddToMap(Variable("a"), StringLiteral("key"), IntLiteral(1)).toSensString should equal ("a[\"key\"] = 1")
  }

  "Variable statements" should "be formatted in Sens correctly" in {
    VariableAssignment("a", IntLiteral(1)).toSensString should equal ("a = 1")

    VariableDefinition("a", Some(IntLiteral(1))).toSensString should equal ("var a = 1")
    VariableDefinition("a", None).toSensString should equal ("var a")

    Increment(Variable("a")).toSensString should equal ("a++")
    Decrement(Variable("a")).toSensString should equal ("a--")
  }

  "break and continue statements" should "be formatted in Sens correctly" in {
    Break().toSensString should equal ("break")
    Continue().toSensString should equal ("continue")
    NOP().toSensString should equal ("")
  }

  "Return statements" should "be formatted in Sens correctly" in {
    Return(Some(Variable("a"))).toSensString should equal ("return a")
    Return(None).toSensString should equal ("return")
    InvisibleReturn(Some(Variable("a"))).toSensString should equal ("a")
    InvisibleReturn(None).toSensString should equal ("")
  }

  "loop statements" should "be formatted in Sens correctly" in {
    val forLoop = For(
      Some(VariableDefinition("i", Some(IntLiteral(0)))),
      Some(LessThan(Variable("i"), IntLiteral(10))),
      Some(Increment(Variable("i"))),
      ProcedureCall(FunctionCall(FunctionReference("println"), Variable("i") :: Nil))
    )
    forLoop.toSensString should equal ("for(var i = 0; (i < 10); i++) println(i)")
    For(None, None, None, ProcedureCall(expression.FunctionCall(FunctionReference("doSomething"), Nil)))
      .toSensString should equal ("for(; ; ) doSomething()")


    val foreachListLoop = ForEachItemInList(
      "curItem",
      Variable("list"),
      ProcedureCall(expression.FunctionCall(FunctionReference("println"), Variable("curItem") :: Nil)))
    foreachListLoop.toSensString should equal ("foreach(curItem in list) println(curItem)")

    val foreachMapLoop = ForEachItemInMap(
      "curKey",
      "curVal",
      Variable("list"),
      ProcedureCall(expression.FunctionCall(FunctionReference("println"), Add(Variable("curKey"), Add(StringLiteral(": "), Variable("curVal"))) :: Nil)))
    foreachMapLoop.toSensString should equal ("foreach(curKey, curVal in list) println((curKey + (\": \" + curVal)))")

    val whileLoop = While(
      Variable("flag"),
      VariableAssignment("flag", expression.FunctionCall(FunctionReference("someFunction"), Nil))
    )
    whileLoop.toSensString should equal ("while flag do flag = someFunction()")

    val doWhileLoop = DoWhile(
      Variable("flag"),
      VariableAssignment("flag", expression.FunctionCall(FunctionReference("someFunction"), Nil))
    )
    doWhileLoop.toSensString should equal ("do flag = someFunction() while flag")
  }

  "If statement" should "be formatted in Sens correctly" in {
    val ifThenElse = If(
      Variable("flag"),
      ProcedureCall(expression.FunctionCall(FunctionReference("doIfTrue"), Nil)),
      Some(ProcedureCall(expression.FunctionCall(FunctionReference("doIfFalse"), Nil)))
    )
    ifThenElse.toSensString should equal ("if flag then doIfTrue() else doIfFalse()")

    val ifThen = If(
      Variable("flag"),
      ProcedureCall(expression.FunctionCall(FunctionReference("doIfTrue"), Nil)),
      None
    )
    ifThen.toSensString should equal ("if flag then doIfTrue()")
  }

  "Functions statements" should "be formatted in Sens correctly" in {
    val funcDef = FunctionDefinition(
      "myFunc",
      "a" :: "b" :: Nil,
      Return(Some(Add(Variable("a"), Variable("b"))))
    )
    funcDef.toSensString should equal ("def myFunc(a, b) return (a + b)")

    val funcDefWithoutArgs = FunctionDefinition(
      "myFunc",
      Nil,
      Return(None)
    )
    funcDefWithoutArgs.toSensString should equal ("def myFunc() return")

    ProcedureCall(expression.FunctionCall(FunctionReference("println"), Variable("i") :: Nil)).toSensString should equal ("println(i)")
    ProcedureCall(expression.FunctionCall(FunctionReference("println"), Nil)).toSensString should equal ("println()")
  }

  "Statement block" should "be formatted in Sens correctly" in {
    StatementBlock(Nil).toSensString should equal ("{\n}")

    val stmtBlock = StatementBlock(
      VariableDefinition("a", Some(Add(Variable("b"), Variable("c")))) ::
      Return(Some(Variable("a"))) ::
      Nil
    )
    val stmtBlockStr = "{\n" +
    "var a = (b + c)\n" +
    "return a\n" +
    "}"
    stmtBlock.toSensString should equal (stmtBlockStr)
  }

  "Concept definition statement" should "be formatted in Sens correctly" in {
    val conceptDef = InheritedConcept.builder(
      "someConcept",
      ParentConcept(ConceptReference("someParentConcept"), Some("c1"), Map(), Nil) :: Nil)
      .build
    ConceptDefinition(conceptDef).toSensString should equal (conceptDef.toSensString)
  }
}
