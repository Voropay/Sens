package org.sens.ast

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.concept.{InheritedConcept, ParentConcept}
import org.sens.core.expression.concept.ConceptReference
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.operation.comparison.LessThan
import org.sens.core.expression.{FunctionCall, Variable}
import org.sens.core.expression.literal.{BooleanLiteral, FloatLiteral, IntLiteral, NullLiteral, StringLiteral}
import org.sens.core.expression.operation.arithmetic.Multiply
import org.sens.core.statement._
import org.sens.parser.WrongTypeException

class StatementOperationsTests extends AnyFlatSpec with Matchers {

  "Loop statements" should "work with subexpressions correctly" in {
    val w = While(LessThan(Variable("a"), IntLiteral(10)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2))))
    w.getSubExpressions should equal(LessThan(Variable("a"), IntLiteral(10)) :: Nil)
    w.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    w.findSubExpression(_ == IntLiteral(10)) should equal(Some(IntLiteral(10)))
    w.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    w.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    w.findAllSubExpressions(_ == IntLiteral(10)) should equal(List(IntLiteral(10)))
    w.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    w.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(w)
    w.replaceSubExpression(IntLiteral(10), IntLiteral(20)) should equal(
      While(LessThan(Variable("a"), IntLiteral(20)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2))))
    )
    w.replaceSubExpression(IntLiteral(2), IntLiteral(3)) should equal(
      While(LessThan(Variable("a"), IntLiteral(10)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(3))))
    )

    val dw = DoWhile(LessThan(Variable("a"), IntLiteral(10)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2))))
    dw.getSubExpressions should equal(LessThan(Variable("a"), IntLiteral(10)) :: Nil)
    dw.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    dw.findSubExpression(_ == IntLiteral(10)) should equal(Some(IntLiteral(10)))
    dw.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    dw.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    dw.findAllSubExpressions(_ == IntLiteral(10)) should equal(List(IntLiteral(10)))
    dw.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    dw.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(dw)
    dw.replaceSubExpression(IntLiteral(10), IntLiteral(20)) should equal(
      DoWhile(LessThan(Variable("a"), IntLiteral(20)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2))))
    )
    dw.replaceSubExpression(IntLiteral(2), IntLiteral(3)) should equal(
      DoWhile(LessThan(Variable("a"), IntLiteral(10)), VariableAssignment("a", Multiply(Variable("a"), IntLiteral(3))))
    )

    val f = For(
      Some(VariableDefinition("a", Some(IntLiteral(1)))),
      Some(LessThan(Variable("a"), IntLiteral(100))),
      Some(VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2)))),
      ProcedureCall(FunctionCall(FunctionReference("println"), Variable("a") :: Nil))
    )
    f.getSubExpressions should equal(LessThan(Variable("a"), IntLiteral(100)) :: Nil)
    f.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    f.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    f.findSubExpression(_ == IntLiteral(100)) should equal(Some(IntLiteral(100)))
    f.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    f.findSubExpression(_ == FunctionReference("println")) should equal(Some(FunctionReference("println")))
    f.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    f.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    f.findAllSubExpressions(_ == IntLiteral(100)) should equal(List(IntLiteral(100)))
    f.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    f.findAllSubExpressions(_ == FunctionReference("println")) should equal(List(FunctionReference("println")))
    f.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(f)
    f.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      For(
        Some(VariableDefinition("a", Some(IntLiteral(2)))),
        Some(LessThan(Variable("a"), IntLiteral(100))),
        Some(VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2)))),
        ProcedureCall(FunctionCall(FunctionReference("println"), Variable("a") :: Nil))
      )
    )
    f.replaceSubExpression(IntLiteral(100), IntLiteral(200)) should equal(
      For(
        Some(VariableDefinition("a", Some(IntLiteral(1)))),
        Some(LessThan(Variable("a"), IntLiteral(200))),
        Some(VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2)))),
        ProcedureCall(FunctionCall(FunctionReference("println"), Variable("a") :: Nil))
      )
    )
    f.replaceSubExpression(IntLiteral(2), IntLiteral(3)) should equal(
      For(
        Some(VariableDefinition("a", Some(IntLiteral(1)))),
        Some(LessThan(Variable("a"), IntLiteral(100))),
        Some(VariableAssignment("a", Multiply(Variable("a"), IntLiteral(3)))),
        ProcedureCall(FunctionCall(FunctionReference("println"), Variable("a") :: Nil))
      )
    )
    f.replaceSubExpression(FunctionReference("println"), FunctionReference("output")) should equal(
      For(
        Some(VariableDefinition("a", Some(IntLiteral(1)))),
        Some(LessThan(Variable("a"), IntLiteral(100))),
        Some(VariableAssignment("a", Multiply(Variable("a"), IntLiteral(2)))),
        ProcedureCall(FunctionCall(FunctionReference("output"), Variable("a") :: Nil))
      )
    )

    val fm = ForEachItemInMap("k", "v", Variable("a"), ProcedureCall(FunctionCall(FunctionReference("println"), Variable("k") :: Nil)))
    fm.getSubExpressions should equal (Variable("a") :: Nil)
    fm.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    fm.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    fm.findSubExpression(_ == Variable("k")) should equal(Some(Variable("k")))
    fm.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    fm.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    fm.findAllSubExpressions(_ == Variable("k")) should equal(List(Variable("k")))
    fm.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(fm)
    fm.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      ForEachItemInMap("k", "v", Variable("b"), ProcedureCall(FunctionCall(FunctionReference("println"), Variable("k") :: Nil)))
    )
    fm.replaceSubExpression(FunctionReference("println"), FunctionReference("output")) should equal(
      ForEachItemInMap("k", "v", Variable("a"), ProcedureCall(FunctionCall(FunctionReference("output"), Variable("k") :: Nil)))
    )

    val fl = ForEachItemInList("i", Variable("a"), ProcedureCall(FunctionCall(FunctionReference("println"), Variable("i") :: Nil)))
    fl.getSubExpressions should equal(Variable("a") :: Nil)
    fl.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    fl.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    fl.findSubExpression(_ == Variable("i")) should equal(Some(Variable("i")))
    fl.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    fl.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    fl.findAllSubExpressions(_ == Variable("i")) should equal(List(Variable("i")))
    fl.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(fl)
    fl.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      ForEachItemInList("i", Variable("b"), ProcedureCall(FunctionCall(FunctionReference("println"), Variable("i") :: Nil)))
    )
    fl.replaceSubExpression(FunctionReference("println"), FunctionReference("output")) should equal(
      ForEachItemInList("i", Variable("a"), ProcedureCall(FunctionCall(FunctionReference("output"), Variable("i") :: Nil)))
    )
  }

  "Variable manipulation statements" should "work with subexpressions correctly" in {
    val vd = VariableDefinition("a", Some(IntLiteral(1)))
    vd.getSubExpressions should equal(IntLiteral(1) :: Nil)
    vd.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    vd.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    vd.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    vd.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    vd.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(vd)
    vd.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      VariableDefinition("a", Some(IntLiteral(2)))
    )

    val va = VariableAssignment("a", IntLiteral(1))
    va.getSubExpressions should equal(IntLiteral(1) :: Nil)
    va.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    va.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    va.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    va.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    va.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(va)
    va.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      VariableAssignment("a", IntLiteral(2))
    )

    val i = Increment(Variable("a"))
    i.getSubExpressions should equal(Variable("a") :: Nil)
    i.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    i.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    i.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    i.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    i.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(i)
    i.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      Increment(Variable("b"))
    )

    val d = Decrement(Variable("a"))
    d.getSubExpressions should equal(Variable("a") :: Nil)
    d.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    d.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    d.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    d.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    d.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(d)
    d.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      Decrement(Variable("b"))
    )

    val am = AddToMap(Variable("a"), StringLiteral("key"), IntLiteral(1))
    am.getSubExpressions should equal(Variable("a") :: StringLiteral("key") :: IntLiteral(1) :: Nil)
    am.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    am.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    am.findSubExpression(_ == StringLiteral("key")) should equal(Some(StringLiteral("key")))
    am.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    am.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    am.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    am.findAllSubExpressions(_ == StringLiteral("key")) should equal(List(StringLiteral("key")))
    am.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    am.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(am)
    am.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      AddToMap(Variable("b"), StringLiteral("key"), IntLiteral(1))
    )
    am.replaceSubExpression(StringLiteral("key"), Variable("b")) should equal(
      AddToMap(Variable("a"), Variable("b"), IntLiteral(1))
    )
    am.replaceSubExpression(IntLiteral(1), Variable("b")) should equal(
      AddToMap(Variable("a"), StringLiteral("key"), Variable("b"))
    )

    val al = AddToList(Variable("a"), IntLiteral(1))
    al.getSubExpressions should equal(Variable("a") :: IntLiteral(1) :: Nil)
    al.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    al.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    al.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    al.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    al.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    al.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    al.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(al)
    al.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      AddToList(Variable("b"), IntLiteral(1))
    )
    al.replaceSubExpression(IntLiteral(1), Variable("b")) should equal(
      AddToList(Variable("a"), Variable("b"))
    )
  }

  "Function statements" should "work with subexpressions correctly" in {
    val fd = FunctionDefinition("square", "a" :: Nil, Return(Some(Multiply(Variable("a"), Variable("a")))))
    fd.getSubExpressions should equal(Nil)
    fd.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    fd.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    fd.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    fd.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a"), Variable("a")))
    fd.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(fd)
    fd.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      FunctionDefinition("square", "a" :: Nil, Return(Some(Multiply(Variable("b"), Variable("b")))))
    )

    val pc = ProcedureCall(FunctionCall(FunctionReference("f"), IntLiteral(1) :: Nil))
    pc.getSubExpressions should equal(FunctionCall(FunctionReference("f"), IntLiteral(1) :: Nil) :: Nil)
    pc.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    pc.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    pc.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    pc.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    pc.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(pc)
    pc.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      ProcedureCall(FunctionCall(FunctionReference("f"), IntLiteral(2) :: Nil))
    )
    assertThrows[WrongTypeException] {
      pc.replaceSubExpression(FunctionCall(FunctionReference("f"), IntLiteral(1) :: Nil), BooleanLiteral(false))
    }

    val c = StandardSensStatements.functions("concat")
    c.getSubExpressions should equal(Nil)
    c.findSubExpression(_ == NullLiteral()) should equal(None)
    c.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    c.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(c)

    val l = StandardSensStatements.functions("length")
    l.getSubExpressions should equal(Nil)
    l.findSubExpression(_ == NullLiteral()) should equal(None)
    l.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    l.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(l)

    val r = StandardSensStatements.functions("round")
    r.getSubExpressions should equal(Nil)
    r.findSubExpression(_ == NullLiteral()) should equal(None)
    r.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    r.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(r)
  }

  "Statement block" should "work with subexpressions correctly" in {
    val s = StatementBlock(VariableDefinition("a", Some(IntLiteral(1))) :: Nil)
    s.getSubExpressions should equal (Nil)
    s.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    s.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    s.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    s.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    s.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(s)
    s.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      StatementBlock(VariableDefinition("a", Some(IntLiteral(2))) :: Nil)
    )
  }

  "Control statements" should "work with subexpressions correctly" in {
    val r = Return(Some(IntLiteral(1)))
    r.getSubExpressions should equal(IntLiteral(1) :: Nil)
    r.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    r.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    r.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    r.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    r.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(r)
    r.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      Return(Some(IntLiteral(2)))
    )

    val ir = InvisibleReturn(Some(IntLiteral(1)))
    ir.getSubExpressions should equal(IntLiteral(1) :: Nil)
    ir.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    ir.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    ir.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    ir.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    ir.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(ir)
    ir.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(
      InvisibleReturn(Some(IntLiteral(2)))
    )

    val n = NOP()
    n.getSubExpressions should equal(Nil)
    n.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    n.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    n.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(n)

    val i = If(Variable("a"), Return(Some(IntLiteral(1))), Some(Return(Some(IntLiteral(2)))))
    i.getSubExpressions should equal(Variable("a") :: Nil)
    i.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    i.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    i.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    i.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    i.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    i.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    i.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    i.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    i.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(i)
    i.replaceSubExpression(Variable("a"), Variable("b")) should equal(
      If(Variable("b"), Return(Some(IntLiteral(1))), Some(Return(Some(IntLiteral(2)))))
    )
    i.replaceSubExpression(IntLiteral(1), IntLiteral(0)) should equal(
      If(Variable("a"), Return(Some(IntLiteral(0))), Some(Return(Some(IntLiteral(2)))))
    )
    i.replaceSubExpression(IntLiteral(2), IntLiteral(0)) should equal(
      If(Variable("a"), Return(Some(IntLiteral(1))), Some(Return(Some(IntLiteral(0)))))
    )

    val c = Continue()
    c.getSubExpressions should equal(Nil)
    c.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    c.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    c.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(c)

    val b = Break()
    b.getSubExpressions should equal(Nil)
    b.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    b.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    b.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(b)
  }

  "Concept definition statement" should "work with subexpressions correctly" in {
    val cd = ConceptDefinition(
      InheritedConcept.builder(
        "someConcept",
        ParentConcept(ConceptReference("anotherConcept"), None, Map(), Nil) :: Nil).build()
    )
    cd.getSubExpressions should equal(Nil)
    cd.findSubExpression(_ == BooleanLiteral(false)) should equal(None)
    cd.findSubExpression(_ == ConceptReference("anotherConcept")) should equal(Some(ConceptReference("anotherConcept")))
    cd.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    cd.findAllSubExpressions(_ == ConceptReference("anotherConcept")) should equal(List(ConceptReference("anotherConcept")))
    cd.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(cd)
    cd.replaceSubExpression(ConceptReference("anotherConcept"), ConceptReference("yetAnotherConcept")) should equal(
      ConceptDefinition(
        InheritedConcept.builder(
          "someConcept",
          ParentConcept(ConceptReference("yetAnotherConcept"), None, Map(), Nil) :: Nil).build()
      )
    )
  }



}
