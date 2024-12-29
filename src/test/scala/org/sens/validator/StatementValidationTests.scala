package org.sens.validator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{Attribute, Concept, InheritedConcept, ParentConcept}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.{IntLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.Add
import org.sens.core.expression.operation.comparison.{Equals, LessThan}
import org.sens.core.expression.{ConceptAttribute, FunctionCall, NamedElementPlaceholder, Variable}
import org.sens.core.statement._
import org.sens.parser.{ElementNotFoundException, ValidationContext}

import scala.util.Failure

class StatementValidationTests extends AnyFlatSpec with Matchers {

  "AddToList statement" should "be validated correctly" in {
    val context = ValidationContext()
    val addToList = AddToList(Variable("a"), NamedElementPlaceholder("b"))
    addToList.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) ::
      VariableDefinition("b", None) :: Nil
    )
    addToList.validateAndRemoveVariablePlaceholders(context).get should equal (
      AddToList(Variable("a"), Variable("b"))
    )
  }

  "AddToMap statement" should "be validated correctly" in {
    val context = ValidationContext()
    val addToMap= AddToMap(Variable("a"), NamedElementPlaceholder("b"), NamedElementPlaceholder("c"))
    addToMap.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) ::
      VariableDefinition("b", None) ::
      VariableDefinition("c", None) :: Nil
    )
    addToMap.validateAndRemoveVariablePlaceholders(context).get should equal (
      AddToMap(Variable("a"), Variable("b"), Variable("c"))
    )
  }

  "Control statements" should "be validated correctly" in {
    val context = ValidationContext()
    Break().validateAndRemoveVariablePlaceholders(context).get should equal (Break())
    Continue().validateAndRemoveVariablePlaceholders(context).get should equal (Continue())
    NOP().validateAndRemoveVariablePlaceholders(context).get should equal (NOP())
  }

  "Increment and decrement statements" should "be validated correctly" in {
    val context = ValidationContext()
    val inc = Increment(Variable("a"))
    val dec = Increment(Variable("a"))
    inc.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    dec.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    context.addVariables(
      VariableDefinition("a", None) :: Nil
    )
    inc.validateAndRemoveVariablePlaceholders(context).get should equal (inc)
    dec.validateAndRemoveVariablePlaceholders(context).get should equal (dec)
  }

  "While statements" should "be validated correctly" in {
    val context = ValidationContext()
    val doWhileStmt = DoWhile(
      LessThan(NamedElementPlaceholder("a"), IntLiteral(10)),
      Increment(Variable("a"))
    )
    val whileStmt = While(
      LessThan(NamedElementPlaceholder("a"), IntLiteral(10)),
      Increment(Variable("a"))
    )
    doWhileStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    whileStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: Nil
    )
    doWhileStmt.validateAndRemoveVariablePlaceholders(context).get should equal (
      DoWhile(
        LessThan(Variable("a"), IntLiteral(10)),
        Increment(Variable("a"))
      )
    )
    whileStmt.validateAndRemoveVariablePlaceholders(context).get should equal (
      While(
        LessThan(Variable("a"), IntLiteral(10)),
        Increment(Variable("a"))
      )
    )
  }

  "For statement" should "be validated correctly" in {
    val context = ValidationContext()
    val forStmt = For(
      Some(VariableDefinition("i", Some(IntLiteral(0)))),
      Some(LessThan(NamedElementPlaceholder("i"), NamedElementPlaceholder("n"))),
      Some(Increment(Variable("i"))),
      ProcedureCall(FunctionCall(
        FunctionReference("println"),
        NamedElementPlaceholder("i") :: Nil
      ))
    )
    forStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("n"))
    )

    context.addVariables(
      VariableDefinition("n", None) :: Nil
    )
    context.addFunction(FunctionDefinition("println", "str" :: Nil, NOP()))
    forStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      For(
        Some(VariableDefinition("i", Some(IntLiteral(0)))),
        Some(LessThan(Variable("i"), Variable("n"))),
        Some(Increment(Variable("i"))),
        ProcedureCall(FunctionCall(
          FunctionReference("println"),
          Variable("i") :: Nil
        ))
      )
    )
  }

  "Foreach item in list statements" should "be validated correctly" in {
    val context = ValidationContext()
    val forStmt = ForEachItemInList(
      "i",
      NamedElementPlaceholder("a"),
      ProcedureCall(FunctionCall(
        FunctionReference("println"),
        NamedElementPlaceholder("i") :: Nil
      ))
    )
    forStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: Nil
    )
    context.addFunction(FunctionDefinition("println", "str" :: Nil, NOP()))
    forStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      ForEachItemInList(
        "i",
        Variable("a"),
        ProcedureCall(FunctionCall(
          FunctionReference("println"),
          Variable("i") :: Nil
        ))
      )
    )
  }

  "Foreach item in map statements" should "be validated correctly" in {
    val context = ValidationContext()
    val forStmt = ForEachItemInMap(
      "k",
      "v",
      NamedElementPlaceholder("a"),
      ProcedureCall(FunctionCall(
        FunctionReference("println"),
        Add(NamedElementPlaceholder("k"), Add(StringLiteral(": "), NamedElementPlaceholder("v"))) :: Nil
      ))
    )
    forStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: Nil
    )
    context.addFunction(FunctionDefinition("println", "str" :: Nil, NOP()))
    forStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      ForEachItemInMap(
        "k",
        "v",
        Variable("a"),
        ProcedureCall(FunctionCall(
          FunctionReference("println"),
          Add(Variable("k"), Add(StringLiteral(": "), Variable("v"))) :: Nil
        ))
      )
    )
  }

  "If statement" should "be validated correctly" in {
    val context = ValidationContext()
    val ifStmt = If(
      NamedElementPlaceholder("a"),
      Increment(Variable("b")),
      Some(Decrement(Variable("b")))
    )
    ifStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil
    )
    ifStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      If(
        Variable("a"),
        Increment(Variable("b")),
        Some(Decrement(Variable("b")))
      )
    )
  }

  "Return statements" should "be validated correctly" in {
    val context = ValidationContext()
    val retStmt = Return(Some(NamedElementPlaceholder("a")))
    retStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )
    val invRetStmt = InvisibleReturn(Some(NamedElementPlaceholder("a")))
    invRetStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: Nil
    )
    retStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      Return(Some(Variable("a")))
    )
    invRetStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      InvisibleReturn(Some(Variable("a")))
    )
  }

  "Procedure call statement" should "be validated correctly" in {
    val context = ValidationContext()
    val callStmt = ProcedureCall(FunctionCall(
      FunctionReference("println"),
      NamedElementPlaceholder("i") :: Nil
    ))
    callStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("println"))
    )

    context.addVariables(
      VariableDefinition("i", None) :: Nil
    )
    context.addFunction(FunctionDefinition("println", "str" :: Nil, NOP()))
    callStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      ProcedureCall(FunctionCall(
        FunctionReference("println"),
        Variable("i") :: Nil
      ))
    )
  }

  "Variable assignment statement" should "be validated correctly" in {
    val context = ValidationContext()
    val varStmt = VariableAssignment("a", NamedElementPlaceholder("b"))
    varStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("a"))
    )

    context.addVariables(
      VariableDefinition("a", None) :: VariableDefinition("b", None) :: Nil
    )
    varStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      VariableAssignment("a", Variable("b"))
    )
  }

  "Variable definition statement" should "be validated correctly" in {
    val context = ValidationContext()
    val varStmt = VariableDefinition("a", Some(NamedElementPlaceholder("b")))
    varStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("b"))
    )

    context.addVariables(
      VariableDefinition("b", None) :: Nil
    )
    varStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      VariableDefinition("a", Some(Variable("b")))
    )
    context.containsVariable("a") should be (true)
  }

  "Function Definition statement" should "be validated correctly" in {
    val context = ValidationContext()
    val funcStmt = FunctionDefinition(
      "doSomething",
      "a" :: Nil,
      Return(Some(FunctionCall(FunctionReference("doSomethingElse"), NamedElementPlaceholder("a") :: Nil)))
    )
    funcStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("doSomethingElse"))
    )

    context.addFunction(FunctionDefinition("doSomethingElse", "arg" :: Nil, NOP()))
    funcStmt.validateAndRemoveVariablePlaceholders(context).get should equal(
      FunctionDefinition(
        "doSomething",
        "a" :: Nil,
        Return(Some(FunctionCall(FunctionReference("doSomethingElse"), Variable("a") :: Nil)))
      )
    )
    context.containsFunction("doSomething") should be (true)
  }

  "Concept Definition statement" should "be validated correctly" in {
    val context = ValidationContext()
    val conDefStmt = ConceptDefinition(InheritedConcept.builder(
      "conceptA",
      ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
      .overriddenAttributes(Attribute("a", None, Nil) :: Nil)
      .additionalDependencies(Equals(NamedElementPlaceholder("a"), IntLiteral(0)))
      .build
    )
    conDefStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("conceptB"))
    )

    context.addConcept(ConceptDefinition(InheritedConcept.builder(
      "conceptB",
      ParentConcept(ConceptReference("conceptC"), None, Map(), Nil) :: Nil)
      .build
    ))

    context.addConcept(ConceptDefinition(Concept.builder(
      "conceptC",
      Attribute("attr1", Some(ConceptAttribute("conceptD" :: Nil, "attr1")), Nil) :: Nil,
      ParentConcept(ConceptReference("conceptD"), None, Map(), Nil) :: Nil)
      .build
    ))

    conDefStmt.validateAndRemoveVariablePlaceholders(context).get should equal (
      ConceptDefinition(InheritedConcept.builder(
        "conceptA",
        ParentConcept(ConceptReference("conceptB"), None, Map(), Nil) :: Nil)
        .overriddenAttributes(Attribute("a", None, Nil) :: Nil)
        .additionalDependencies(Equals(ConceptAttribute(Nil, "a"), IntLiteral(0)))
        .build
      ))
    context.containsConcept("conceptA") should be (true)
  }

  "Statement block" should "be validated correctly" in {
    val context = ValidationContext()
    val blockStmt = StatementBlock(
      VariableDefinition("a", Some(FunctionCall(FunctionReference("random"), Nil))) ::
      Return(Some(NamedElementPlaceholder("a"))) :: Nil
    )
    blockStmt.validateAndRemoveVariablePlaceholders(context) should equal(
      Failure(ElementNotFoundException("random"))
    )

    context.addFunction(FunctionDefinition("random", Nil, NOP()))
    blockStmt.validateAndRemoveVariablePlaceholders(context).get should equal (
      StatementBlock(
        VariableDefinition("a", Some(FunctionCall(FunctionReference("random"), Nil))) ::
        Return(Some(Variable("a"))) :: Nil
      )
    )
  }



}
