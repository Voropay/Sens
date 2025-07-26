package org.sens.ast

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression.concept._
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.core.expression.{CollectionItem, ConceptAttribute, ListInitialization, Variable, _}
import org.sens.core.expression.function._
import org.sens.parser.WrongTypeException
import org.sens.core.statement.Return
import org.sens.core.concept.{Annotation, Attribute, Order, ParentConcept}

class ExpressionOperationsTests extends AnyFlatSpec with Matchers {

  "Literals" should "work with subexpressions correctly" in {
    val b = BooleanLiteral(true)
    b.getSubExpressions should equal (Nil)
    b.findSubExpression(_ == BooleanLiteral(true)) should equal (Some(b))
    b.findSubExpression(_ == BooleanLiteral(false)) should equal (None)
    b.findAllSubExpressions(_ == BooleanLiteral(true)) should equal(List(b))
    b.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(Nil)
    b.replaceSubExpression(BooleanLiteral(true), IntLiteral(1)) should equal (IntLiteral(1))
    b.replaceSubExpression(BooleanLiteral(false), IntLiteral(1)) should equal (b)

    val i = IntLiteral(1)
    i.getSubExpressions should equal(Nil)
    i.findSubExpression(_ == IntLiteral(1)) should equal(Some(i))
    i.findSubExpression(_ == IntLiteral(0)) should equal(None)
    i.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(i))
    i.findAllSubExpressions(_ == IntLiteral(0)) should equal(Nil)
    i.replaceSubExpression(IntLiteral(1), IntLiteral(2)) should equal(IntLiteral(2))
    i.replaceSubExpression(BooleanLiteral(false), IntLiteral(2)) should equal(i)

    val f = FloatLiteral(1)
    f.getSubExpressions should equal(Nil)
    f.findSubExpression(_ == FloatLiteral(1)) should equal(Some(f))
    f.findSubExpression(_ == FloatLiteral(0)) should equal(None)
    f.findAllSubExpressions(_ == FloatLiteral(1)) should equal(List(f))
    f.findAllSubExpressions(_ == FloatLiteral(0)) should equal(Nil)
    f.replaceSubExpression(FloatLiteral(1), FloatLiteral(2)) should equal(FloatLiteral(2))
    f.replaceSubExpression(BooleanLiteral(false), FloatLiteral(2)) should equal(f)

    val s = StringLiteral("1")
    s.getSubExpressions should equal(Nil)
    s.findSubExpression(_ == StringLiteral("1")) should equal(Some(s))
    s.findSubExpression(_ == StringLiteral("0")) should equal(None)
    s.findAllSubExpressions(_ == StringLiteral("1")) should equal(List(s))
    s.findAllSubExpressions(_ == StringLiteral("0")) should equal(Nil)
    s.replaceSubExpression(StringLiteral("1"), FloatLiteral(1)) should equal(FloatLiteral(1))
    s.replaceSubExpression(BooleanLiteral(false), FloatLiteral(1)) should equal(s)

    val n = NullLiteral()
    n.getSubExpressions should equal(Nil)
    n.findSubExpression(_ == NullLiteral()) should equal(Some(n))
    n.findSubExpression(_ == StringLiteral("0")) should equal(None)
    n.findAllSubExpressions(_ == NullLiteral()) should equal(List(n))
    n.findAllSubExpressions(_ == StringLiteral("0")) should equal(Nil)
    n.replaceSubExpression(NullLiteral(), FloatLiteral(0)) should equal(FloatLiteral(0))
    n.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(n)

    val l = ListLiteral(BooleanLiteral(true) :: IntLiteral(1) :: FloatLiteral(1) :: Nil)
    l.getSubExpressions should equal(BooleanLiteral(true) :: IntLiteral(1) :: FloatLiteral(1) :: Nil)
    l.findSubExpression(_ == NullLiteral()) should equal(None)
    l.findSubExpression(_ == l) should equal(Some(l))
    l.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    l.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    l.findAllSubExpressions(_ == l) should equal(List(l))
    l.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    l.replaceSubExpression(l, FloatLiteral(0)) should equal(FloatLiteral(0))
    l.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(l)
    l.replaceSubExpression(BooleanLiteral(true), FloatLiteral(0)) should equal(
      ListLiteral(FloatLiteral(0) :: IntLiteral(1) :: FloatLiteral(1) :: Nil)
    )
    assertThrows[WrongTypeException] {
      l.replaceSubExpression(BooleanLiteral(true), Variable("a"))
    }

    val m = MapLiteral(Map(IntLiteral(1) -> StringLiteral("1"), IntLiteral(2) -> StringLiteral("2")))
    m.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: StringLiteral("1") :: StringLiteral("2") :: Nil)
    m.findSubExpression(_ == NullLiteral()) should equal(None)
    m.findSubExpression(_ == m) should equal(Some(m))
    m.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    m.findSubExpression(_ == StringLiteral("2")) should equal(Some(StringLiteral("2")))
    m.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    m.findAllSubExpressions(_ == m) should equal(List(m))
    m.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    m.findAllSubExpressions(_ == StringLiteral("2")) should equal(List(StringLiteral("2")))
    m.replaceSubExpression(m, FloatLiteral(0)) should equal(FloatLiteral(0))
    m.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(m)
    m.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      MapLiteral(Map(FloatLiteral(1) -> StringLiteral("1"), IntLiteral(2) -> StringLiteral("2")))
    )
    m.replaceSubExpression(StringLiteral("1"), FloatLiteral(1)) should equal(
      MapLiteral(Map(IntLiteral(1) -> FloatLiteral(1), IntLiteral(2) -> StringLiteral("2")))
    )
    assertThrows[WrongTypeException] {
      m.replaceSubExpression(StringLiteral("1"), Variable("a"))
    }
  }

  "Type Literals" should "work with subexpressions correctly" in {
    val it = IntTypeLiteral()
    it.getSubExpressions should equal(Nil)
    it.findSubExpression(_ == IntTypeLiteral()) should equal(Some(it))
    it.findSubExpression(_ == FloatTypeLiteral()) should equal(None)
    it.findAllSubExpressions(_ == IntTypeLiteral()) should equal(List(it))
    it.findAllSubExpressions(_ == FloatTypeLiteral()) should equal(Nil)
    it.replaceSubExpression(IntTypeLiteral(), FloatTypeLiteral()) should equal(FloatTypeLiteral())
    it.replaceSubExpression(FloatTypeLiteral(), BooleanTypeLiteral()) should equal(it)

    val ft = FloatTypeLiteral()
    ft.getSubExpressions should equal(Nil)
    ft.findSubExpression(_ == FloatTypeLiteral()) should equal(Some(ft))
    ft.findSubExpression(_ == IntTypeLiteral()) should equal(None)
    ft.findAllSubExpressions(_ == FloatTypeLiteral()) should equal(List(ft))
    ft.findAllSubExpressions(_ == IntTypeLiteral()) should equal(Nil)
    ft.replaceSubExpression(FloatTypeLiteral(), IntTypeLiteral()) should equal(IntTypeLiteral())
    ft.replaceSubExpression(IntTypeLiteral(), FloatTypeLiteral()) should equal(ft)

    val bt = BooleanTypeLiteral()
    bt.getSubExpressions should equal(Nil)
    bt.findSubExpression(_ == BooleanTypeLiteral()) should equal(Some(bt))
    bt.findSubExpression(_ == IntTypeLiteral()) should equal(None)
    bt.findAllSubExpressions(_ == BooleanTypeLiteral()) should equal(List(bt))
    bt.findAllSubExpressions(_ == IntTypeLiteral()) should equal(Nil)
    bt.replaceSubExpression(BooleanTypeLiteral(), IntTypeLiteral()) should equal(IntTypeLiteral())
    bt.replaceSubExpression(IntTypeLiteral(), FloatTypeLiteral()) should equal(bt)

    val st = StringTypeLiteral(255)
    st.getSubExpressions should equal(Nil)
    st.findSubExpression(_ == StringTypeLiteral(255)) should equal(Some(st))
    st.findSubExpression(_ == IntTypeLiteral()) should equal(None)
    st.findAllSubExpressions(_ == StringTypeLiteral(255)) should equal(List(st))
    st.findAllSubExpressions(_ == IntTypeLiteral()) should equal(Nil)
    st.replaceSubExpression(StringTypeLiteral(255), StringTypeLiteral(64)) should equal(StringTypeLiteral(64))
    st.replaceSubExpression(IntTypeLiteral(), BooleanTypeLiteral()) should equal(st)

    val lt = ListTypeLiteral(StringTypeLiteral(255))
    lt.getSubExpressions should equal (StringTypeLiteral(255) :: Nil)
    lt.findSubExpression(_ == ListTypeLiteral(StringTypeLiteral(255))) should equal(Some(lt))
    lt.findSubExpression(_ == StringTypeLiteral(255)) should equal(Some(StringTypeLiteral(255)))
    lt.findSubExpression(_ == IntTypeLiteral()) should equal(None)
    lt.findAllSubExpressions(_ == ListTypeLiteral(StringTypeLiteral(255))) should equal(List(lt))
    lt.findAllSubExpressions(_ == StringTypeLiteral(255)) should equal(List(StringTypeLiteral(255)))
    lt.findAllSubExpressions(_ == IntTypeLiteral()) should equal(Nil)
    lt.replaceSubExpression(StringTypeLiteral(255), FloatTypeLiteral()) should equal(ListTypeLiteral(FloatTypeLiteral()))
    lt.replaceSubExpression(ListTypeLiteral(StringTypeLiteral(255)), FloatTypeLiteral()) should equal(FloatTypeLiteral())
    lt.replaceSubExpression(IntTypeLiteral(), FloatTypeLiteral()) should equal(lt)

    val mt = MapTypeLiteral(Map("attr1" -> StringTypeLiteral(255), "attr2" -> IntTypeLiteral()))
    mt.getSubExpressions should equal(StringTypeLiteral(255) :: IntTypeLiteral() :: Nil)
    mt.findSubExpression(_ == MapTypeLiteral(Map("attr1" -> StringTypeLiteral(255), "attr2" -> IntTypeLiteral()))) should equal(Some(mt))
    mt.findSubExpression(_ == StringTypeLiteral(255)) should equal(Some(StringTypeLiteral(255)))
    mt.findSubExpression(_ == FloatTypeLiteral()) should equal(None)
    mt.findAllSubExpressions(_ == MapTypeLiteral(Map("attr1" -> StringTypeLiteral(255), "attr2" -> IntTypeLiteral()))) should equal(List(mt))
    mt.findAllSubExpressions(_ == StringTypeLiteral(255)) should equal(List(StringTypeLiteral(255)))
    mt.findAllSubExpressions(_ == FloatTypeLiteral()) should equal(Nil)
    mt.replaceSubExpression(StringTypeLiteral(255), FloatTypeLiteral()) should equal(MapTypeLiteral(Map("attr1" -> FloatTypeLiteral(), "attr2" -> IntTypeLiteral())))
    mt.replaceSubExpression(MapTypeLiteral(Map("attr1" -> StringTypeLiteral(255), "attr2" -> IntTypeLiteral())), FloatTypeLiteral()) should equal(FloatTypeLiteral())
    mt.replaceSubExpression(FloatTypeLiteral(), IntTypeLiteral()) should equal(mt)
  }

  "Arithmetic operations" should "work with subexpressions correctly" in {
    val a = Add(IntLiteral(1), IntLiteral(2))
    a.getSubExpressions should equal (IntLiteral(1) :: IntLiteral(2) :: Nil)
    a.findSubExpression(_ == NullLiteral()) should equal(None)
    a.findSubExpression(_ == a) should equal(Some(a))
    a.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    a.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    a.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    a.findAllSubExpressions(_ == a) should equal(List(a))
    a.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    a.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    a.replaceSubExpression(a, IntLiteral(3)) should equal(IntLiteral(3))
    a.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(a)
    a.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Add(FloatLiteral(1), IntLiteral(2))
    )
    a.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Add(IntLiteral(1), FloatLiteral(2))
    )

    val s = Substract(IntLiteral(1), IntLiteral(2))
    s.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    s.findSubExpression(_ == NullLiteral()) should equal(None)
    s.findSubExpression(_ == s) should equal(Some(s))
    s.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    s.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    s.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    s.findAllSubExpressions(_ == s) should equal(List(s))
    s.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    s.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    s.replaceSubExpression(s, IntLiteral(-1)) should equal(IntLiteral(-1))
    s.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(s)
    s.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Substract(FloatLiteral(1), IntLiteral(2))
    )
    s.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Substract(IntLiteral(1), FloatLiteral(2))
    )

    val m = Multiply(IntLiteral(1), IntLiteral(2))
    m.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    m.findSubExpression(_ == NullLiteral()) should equal(None)
    m.findSubExpression(_ == m) should equal(Some(m))
    m.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    m.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    m.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    m.findAllSubExpressions(_ == m) should equal(List(m))
    m.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    m.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    m.replaceSubExpression(m, IntLiteral(2)) should equal(IntLiteral(2))
    m.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(m)
    m.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Multiply(FloatLiteral(1), IntLiteral(2))
    )
    m.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Multiply(IntLiteral(1), FloatLiteral(2))
    )

    val d = Divide(IntLiteral(1), IntLiteral(2))
    d.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    d.findSubExpression(_ == NullLiteral()) should equal(None)
    d.findSubExpression(_ == d) should equal(Some(d))
    d.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    d.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    d.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    d.findAllSubExpressions(_ == d) should equal(List(d))
    d.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    d.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    d.replaceSubExpression(d, FloatLiteral(0.5)) should equal(FloatLiteral(0.5))
    d.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(d)
    d.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Divide(FloatLiteral(1), IntLiteral(2))
    )
    d.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Divide(IntLiteral(1), FloatLiteral(2))
    )

    val um = UnaryMinus(Variable("a"))
    um.getSubExpressions should equal (Variable("a") :: Nil)
    um.findSubExpression(_ == NullLiteral()) should equal(None)
    um.findSubExpression(_ == um) should equal(Some(um))
    um.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    um.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    um.findAllSubExpressions(_ == um) should equal(List(um))
    um.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    um.replaceSubExpression(um, FloatLiteral(0.5)) should equal(FloatLiteral(0.5))
    um.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(um)
    um.replaceSubExpression(Variable("a"), FloatLiteral(1)) should equal(
      UnaryMinus(FloatLiteral(1))
    )
  }

  "Comparison operations" should "work with subexpressions correctly" in {
    val e = Equals(IntLiteral(1), IntLiteral(2))
    e.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    e.findSubExpression(_ == NullLiteral()) should equal(None)
    e.findSubExpression(_ == e) should equal(Some(e))
    e.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    e.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    e.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    e.findAllSubExpressions(_ == e) should equal(List(e))
    e.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    e.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    e.replaceSubExpression(e, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    e.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(e)
    e.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Equals(FloatLiteral(1), IntLiteral(2))
    )
    e.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Equals(IntLiteral(1), FloatLiteral(2))
    )

    val ge = GreaterOrEqualsThan(IntLiteral(1), IntLiteral(2))
    ge.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    ge.findSubExpression(_ == NullLiteral()) should equal(None)
    ge.findSubExpression(_ == ge) should equal(Some(ge))
    ge.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    ge.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    ge.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    ge.findAllSubExpressions(_ == ge) should equal(List(ge))
    ge.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    ge.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    ge.replaceSubExpression(ge, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    ge.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(ge)
    ge.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      GreaterOrEqualsThan(FloatLiteral(1), IntLiteral(2))
    )
    ge.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      GreaterOrEqualsThan(IntLiteral(1), FloatLiteral(2))
    )

    val g = GreaterThan(IntLiteral(1), IntLiteral(2))
    g.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    g.findSubExpression(_ == NullLiteral()) should equal(None)
    g.findSubExpression(_ == g) should equal(Some(g))
    g.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    g.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    g.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    g.findAllSubExpressions(_ == g) should equal(List(g))
    g.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    g.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    g.replaceSubExpression(g, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    g.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(g)
    g.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      GreaterThan(FloatLiteral(1), IntLiteral(2))
    )
    g.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      GreaterThan(IntLiteral(1), FloatLiteral(2))
    )

    val le = LessOrEqualsThan(IntLiteral(1), IntLiteral(2))
    le.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    le.findSubExpression(_ == NullLiteral()) should equal(None)
    le.findSubExpression(_ == le) should equal(Some(le))
    le.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    le.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    le.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    le.findAllSubExpressions(_ == le) should equal(List(le))
    le.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    le.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    le.replaceSubExpression(le, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    le.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(le)
    le.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      LessOrEqualsThan(FloatLiteral(1), IntLiteral(2))
    )
    le.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      LessOrEqualsThan(IntLiteral(1), FloatLiteral(2))
    )

    val l = LessThan(IntLiteral(1), IntLiteral(2))
    l.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    l.findSubExpression(_ == NullLiteral()) should equal(None)
    l.findSubExpression(_ == l) should equal(Some(l))
    l.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    l.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    l.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    l.findAllSubExpressions(_ == l) should equal(List(l))
    l.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    l.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    l.replaceSubExpression(l, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    l.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(l)
    l.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      LessThan(FloatLiteral(1), IntLiteral(2))
    )
    l.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      LessThan(IntLiteral(1), FloatLiteral(2))
    )

    val ne = NotEquals(IntLiteral(1), IntLiteral(2))
    ne.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: Nil)
    ne.findSubExpression(_ == NullLiteral()) should equal(None)
    ne.findSubExpression(_ == ne) should equal(Some(ne))
    ne.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    ne.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    ne.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    ne.findAllSubExpressions(_ == ne) should equal(List(ne))
    ne.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    ne.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    ne.replaceSubExpression(ne, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    ne.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(ne)
    ne.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      NotEquals(FloatLiteral(1), IntLiteral(2))
    )
    ne.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      NotEquals(IntLiteral(1), FloatLiteral(2))
    )

    val b = Between(IntLiteral(1), IntLiteral(2), IntLiteral(3))
    b.getSubExpressions should equal(IntLiteral(1) :: IntLiteral(2) :: IntLiteral(3) :: Nil)
    b.findSubExpression(_ == NullLiteral()) should equal(None)
    b.findSubExpression(_ == b) should equal(Some(b))
    b.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    b.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    b.findSubExpression(_ == IntLiteral(3)) should equal(Some(IntLiteral(3)))
    b.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    b.findAllSubExpressions(_ == b) should equal(List(b))
    b.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    b.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    b.findAllSubExpressions(_ == IntLiteral(3)) should equal(List(IntLiteral(3)))
    b.replaceSubExpression(b, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    b.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(b)
    b.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      Between(FloatLiteral(1), IntLiteral(2), IntLiteral(3))
    )
    b.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(
      Between(IntLiteral(1), FloatLiteral(2), IntLiteral(3))
    )
    b.replaceSubExpression(IntLiteral(3), FloatLiteral(3)) should equal(
      Between(IntLiteral(1), IntLiteral(2), FloatLiteral(3))
    )
  }

  "Logical operations" should "work with subexpressions correctly" in {
    val a = And(BooleanLiteral(true), BooleanLiteral(false))
    a.getSubExpressions should equal(BooleanLiteral(true) :: BooleanLiteral(false) :: Nil)
    a.findSubExpression(_ == NullLiteral()) should equal(None)
    a.findSubExpression(_ == a) should equal(Some(a))
    a.findSubExpression(_ == BooleanLiteral(true)) should equal(Some(BooleanLiteral(true)))
    a.findSubExpression(_ == BooleanLiteral(false)) should equal(Some(BooleanLiteral(false)))
    a.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    a.findAllSubExpressions(_ == a) should equal(List(a))
    a.findAllSubExpressions(_ == BooleanLiteral(true)) should equal(List(BooleanLiteral(true)))
    a.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(List(BooleanLiteral(false)))
    a.replaceSubExpression(a, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    a.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(a)
    a.replaceSubExpression(BooleanLiteral(true), BooleanLiteral(false)) should equal(
      And(BooleanLiteral(false), BooleanLiteral(false))
    )
    a.replaceSubExpression(BooleanLiteral(false), BooleanLiteral(true)) should equal(
      And(BooleanLiteral(true), BooleanLiteral(true))
    )

    val o = Or(BooleanLiteral(true), BooleanLiteral(false))
    o.getSubExpressions should equal(BooleanLiteral(true) :: BooleanLiteral(false) :: Nil)
    o.findSubExpression(_ == NullLiteral()) should equal(None)
    o.findSubExpression(_ == o) should equal(Some(o))
    o.findSubExpression(_ == BooleanLiteral(true)) should equal(Some(BooleanLiteral(true)))
    o.findSubExpression(_ == BooleanLiteral(false)) should equal(Some(BooleanLiteral(false)))
    o.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    o.findAllSubExpressions(_ == o) should equal(List(o))
    o.findAllSubExpressions(_ == BooleanLiteral(true)) should equal(List(BooleanLiteral(true)))
    o.findAllSubExpressions(_ == BooleanLiteral(false)) should equal(List(BooleanLiteral(false)))
    o.replaceSubExpression(o, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    o.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(o)
    o.replaceSubExpression(BooleanLiteral(true), BooleanLiteral(false)) should equal(
      Or(BooleanLiteral(false), BooleanLiteral(false))
    )
    o.replaceSubExpression(BooleanLiteral(false), BooleanLiteral(true)) should equal(
      Or(BooleanLiteral(true), BooleanLiteral(true))
    )

    val n = Not(BooleanLiteral(true))
    n.getSubExpressions should equal(BooleanLiteral(true) :: Nil)
    n.findSubExpression(_ == NullLiteral()) should equal(None)
    n.findSubExpression(_ == n) should equal(Some(n))
    n.findSubExpression(_ == BooleanLiteral(true)) should equal(Some(BooleanLiteral(true)))
    n.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    n.findAllSubExpressions(_ == n) should equal(List(n))
    n.findAllSubExpressions(_ == BooleanLiteral(true)) should equal(List(BooleanLiteral(true)))
    n.replaceSubExpression(n, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    n.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(n)
    n.replaceSubExpression(BooleanLiteral(true), BooleanLiteral(false)) should equal(
      Not(BooleanLiteral(false))
    )

    val as = AndSeq(BooleanLiteral(true) :: BooleanLiteral(false) :: Nil)
    as.getSubExpressions should equal(BooleanLiteral(true) :: BooleanLiteral(false) :: Nil)
    as.findSubExpression(_ == NullLiteral()) should equal(None)
    as.findSubExpression(_ == as) should equal(Some(as))
    as.findSubExpression(_ == BooleanLiteral(true)) should equal(Some(BooleanLiteral(true)))
    as.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    as.findAllSubExpressions(_ == as) should equal(List(as))
    as.findAllSubExpressions(_ == BooleanLiteral(true)) should equal(List(BooleanLiteral(true)))
    as.replaceSubExpression(as, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    as.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(as)
    as.replaceSubExpression(BooleanLiteral(true), BooleanLiteral(false)) should equal(
      AndSeq(BooleanLiteral(false) :: BooleanLiteral(false) :: Nil)
    )
  }

  "Relational operations" should "work with subexpressions correctly" in {

    val conDef1 = AnonymousConceptDefinition.builder(
      Attribute("value", None, Nil) :: Nil,
      ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
    ).build()
    val conDef2 = AnonymousConceptDefinition.builder(
      Attribute("value", None, Nil) :: Nil,
      ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
    ).attributeDependencies(Equals(ConceptAttribute(Nil, "category"), Variable("a")))
      .build()

    val all = All(GreaterThan(ConceptAttribute(Nil, "price"), conDef1))
    all.getSubExpressions should equal(GreaterThan(ConceptAttribute(Nil, "price"), conDef1) :: Nil)
    all.findSubExpression(_ == all) should equal(Some(all))
    all.findSubExpression(_ == NullLiteral()) should equal(None)
    all.findSubExpression(_ == ConceptAttribute(Nil, "price")) should equal (Some(ConceptAttribute(Nil, "price")))
    all.findAllSubExpressions(_ == all) should equal(List(all))
    all.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    all.findAllSubExpressions(_ == ConceptAttribute(Nil, "price")) should equal(List(ConceptAttribute(Nil, "price")))
    all.replaceSubExpression(all, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    all.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(all)
    all.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(all)
    all.replaceSubExpression(ConceptAttribute(Nil, "price"), Variable("price")) should equal(
      All(GreaterThan(Variable("price"), conDef1))
    )
    assertThrows[WrongTypeException] {
      all.replaceSubExpression(GreaterThan(ConceptAttribute(Nil, "price"), conDef1), BooleanLiteral(true))
    }

    val any = Any(GreaterThan(ConceptAttribute(Nil, "price"), conDef1))
    any.getSubExpressions should equal(GreaterThan(ConceptAttribute(Nil, "price"), conDef1) :: Nil)
    any.findSubExpression(_ == any) should equal(Some(any))
    any.findSubExpression(_ == NullLiteral()) should equal(None)
    any.findSubExpression(_ == ConceptAttribute(Nil, "price")) should equal(Some(ConceptAttribute(Nil, "price")))
    any.findAllSubExpressions(_ == any) should equal(List(any))
    any.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    any.findAllSubExpressions(_ == ConceptAttribute(Nil, "price")) should equal(List(ConceptAttribute(Nil, "price")))
    any.replaceSubExpression(any, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    any.replaceSubExpression(ConceptAttribute(Nil, "price"), Variable("price")) should equal(
      Any(GreaterThan(Variable("price"), conDef1))
    )
    assertThrows[WrongTypeException] {
      any.replaceSubExpression(GreaterThan(ConceptAttribute(Nil, "price"), conDef1), BooleanLiteral(true))
    }

    val e = Exists(conDef1)
    e.getSubExpressions should equal (conDef1 :: Nil)
    e.findSubExpression(_ == e) should equal(Some(e))
    e.findSubExpression(_ == NullLiteral()) should equal(None)
    e.findSubExpression(_ == ConceptReference("prices")) should equal(Some(ConceptReference("prices")))
    e.findAllSubExpressions(_ == e) should equal(List(e))
    e.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    e.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    e.replaceSubExpression(e, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    e.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(e)
    e.replaceSubExpression(conDef1, conDef2) should equal(
      Exists(conDef2)
    )
    assertThrows[WrongTypeException] {
      e.replaceSubExpression(conDef1, BooleanLiteral(true))
    }

    val u = Unique(conDef1)
    u.getSubExpressions should equal(conDef1 :: Nil)
    u.findSubExpression(_ == u) should equal(Some(u))
    u.findSubExpression(_ == NullLiteral()) should equal(None)
    u.findSubExpression(_ == ConceptReference("prices")) should equal(Some(ConceptReference("prices")))
    u.findAllSubExpressions(_ == u) should equal(List(u))
    u.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    u.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    u.replaceSubExpression(u, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    u.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(u)
    u.replaceSubExpression(conDef1, conDef2) should equal(
      Unique(conDef2)
    )
    assertThrows[WrongTypeException] {
      u.replaceSubExpression(conDef1, BooleanLiteral(true))
    }

    val il = InList(ConceptAttribute(Nil, "price"), ListInitialization(FloatLiteral(1) :: FloatLiteral(2) :: Nil))
    il.getSubExpressions should equal(ConceptAttribute(Nil, "price") :: ListInitialization(FloatLiteral(1) :: FloatLiteral(2) :: Nil) :: Nil)
    il.findSubExpression(_ == il) should equal(Some(il))
    il.findSubExpression(_ == NullLiteral()) should equal(None)
    il.findSubExpression(_ == ConceptAttribute(Nil, "price")) should equal(Some(ConceptAttribute(Nil, "price")))
    il.findSubExpression(_ == FloatLiteral(1)) should equal(Some(FloatLiteral(1)))
    il.findAllSubExpressions(_ == il) should equal(List(il))
    il.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    il.findAllSubExpressions(_ == ConceptAttribute(Nil, "price")) should equal(List(ConceptAttribute(Nil, "price")))
    il.findAllSubExpressions(_ == FloatLiteral(1)) should equal(List(FloatLiteral(1)))
    il.replaceSubExpression(il, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    il.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(il)
    il.replaceSubExpression(ConceptAttribute(Nil, "price"), Variable("price")) should equal(
      InList(Variable("price"), ListInitialization(FloatLiteral(1) :: FloatLiteral(2) :: Nil))
    )
    il.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(
      InList(ConceptAttribute(Nil, "price"), ListInitialization(FloatLiteral(0) :: FloatLiteral(2) :: Nil))
    )
    assertThrows[WrongTypeException] {
      il.replaceSubExpression(ListInitialization(FloatLiteral(1) :: FloatLiteral(2) :: Nil), BooleanLiteral(true))
    }

    val is = InSubQuery(ConceptAttribute(Nil, "price"), conDef1)
    is.getSubExpressions should equal(ConceptAttribute(Nil, "price") :: conDef1 :: Nil)
    is.findSubExpression(_ == is) should equal(Some(is))
    is.findSubExpression(_ == NullLiteral()) should equal(None)
    is.findSubExpression(_ == ConceptAttribute(Nil, "price")) should equal(Some(ConceptAttribute(Nil, "price")))
    is.findSubExpression(_ == ConceptReference("prices")) should equal(Some(ConceptReference("prices")))
    is.findAllSubExpressions(_ == is) should equal(List(is))
    is.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    is.findAllSubExpressions(_ == ConceptAttribute(Nil, "price")) should equal(List(ConceptAttribute(Nil, "price")))
    is.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    is.replaceSubExpression(is, BooleanLiteral(true)) should equal(BooleanLiteral(true))
    is.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(is)
    is.replaceSubExpression(ConceptAttribute(Nil, "price"), Variable("price")) should equal(
      InSubQuery(Variable("price"), conDef1)
    )
    is.replaceSubExpression(conDef1, conDef2) should equal(
      InSubQuery(ConceptAttribute(Nil, "price"), conDef2)
    )
    assertThrows[WrongTypeException] {
      is.replaceSubExpression(conDef1, BooleanLiteral(true))
    }
  }

  "Function expressions" should "work with subexpressions correctly" in {
    val r = FunctionReference("f")
    r.getSubExpressions should equal(Nil)
    r.findSubExpression(_ == r) should equal(Some(r))
    r.findSubExpression(_ == StringLiteral("0")) should equal(None)
    r.findAllSubExpressions(_ == r) should equal(List(r))
    r.findAllSubExpressions(_ == StringLiteral("0")) should equal(Nil)
    r.replaceSubExpression(r, FloatLiteral(0)) should equal(FloatLiteral(0))
    r.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(r)

    val d = AnonymousFunctionDefinition(Nil, Return(Some(IntLiteral(1))))
    d.getSubExpressions should equal(Nil)
    d.findSubExpression(_ == NullLiteral()) should equal(None)
    d.findSubExpression(_ == d) should equal(Some(d))
    d.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    d.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    d.findAllSubExpressions(_ == d) should equal(List(d))
    d.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    d.replaceSubExpression(d, BooleanLiteral(false)) should equal(BooleanLiteral(false))
    d.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(d)
    d.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(
      AnonymousFunctionDefinition(Nil, Return(Some(FloatLiteral(1))))
    )
  }

  "Concept expressions" should "work with subexpressions correctly" in {
    val conDef = AnonymousConceptDefinition.builder(
      Attribute("value", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
        Attribute("category", None, Nil) :: Nil,
      ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
    ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
      .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
      .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)))
      .orderByAttributes(Order(ConceptAttribute(Nil, "value"), Order.DESC) :: Nil)
      .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
      .build()

    conDef.getSubExpressions should equal (
      Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")) ::
        ConceptAttribute(Nil, "category") ::
        GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)) :: Nil)
    conDef.findSubExpression(_ == NullLiteral()) should equal(None)
    conDef.findSubExpression(_ == conDef) should equal(Some(conDef))
    conDef.findSubExpression(_ == FunctionReference("avg")) should equal(Some(FunctionReference("avg")))
    conDef.findSubExpression(_ == ConceptReference("prices")) should equal(Some(ConceptReference("prices")))
    conDef.findSubExpression(_ == StringLiteral("ca")) should equal(Some(StringLiteral("ca")))
    conDef.findSubExpression(_ == FloatLiteral(10)) should equal(Some(FloatLiteral(10)))
    conDef.findSubExpression(_ == ConceptAttribute(Nil, "value")) should equal(Some(ConceptAttribute(Nil, "value")))
    conDef.findSubExpression(_ == StringLiteral("refactor it")) should equal(Some(StringLiteral("refactor it")))
    conDef.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    conDef.findAllSubExpressions(_ == conDef) should equal(List(conDef))
    conDef.findAllSubExpressions(_ == FunctionReference("avg")) should equal(List(FunctionReference("avg")))
    conDef.findAllSubExpressions(_ == ConceptReference("prices")) should equal(List(ConceptReference("prices")))
    conDef.findAllSubExpressions(_ == StringLiteral("ca")) should equal(List(StringLiteral("ca")))
    conDef.findAllSubExpressions(_ == FloatLiteral(10)) should equal(List(FloatLiteral(10)))
    conDef.findAllSubExpressions(_ == ConceptAttribute(Nil, "value")) should equal(List(ConceptAttribute(Nil, "value")))
    conDef.findAllSubExpressions(_ == StringLiteral("refactor it")) should equal(List(StringLiteral("refactor it")))

    val conDefToReplace = AnonymousConceptDefinition.builder(
      Attribute("priceAvg", None, Nil) ::
        Attribute("category", None, Nil) :: Nil,
      ParentConcept(ConceptReference("pricesStat"), None, Map(), Nil) :: Nil
    ).build()
    conDef.replaceSubExpression(conDef, conDefToReplace) should equal(conDefToReplace)
    conDef.replaceSubExpression(FloatLiteral(1), FloatLiteral(0)) should equal(conDef)
    conDef.replaceSubExpression(FunctionReference("avg"), FunctionReference("max")) should equal(
      AnonymousConceptDefinition.builder(
        Attribute("value", Some(FunctionCall(FunctionReference("max"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
          Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
      ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
        .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "value"), Order.DESC) :: Nil)
        .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
        .build()
    )
    conDef.replaceSubExpression(ConceptReference("prices"), ConceptReference("pricesNew")) should equal(
      AnonymousConceptDefinition.builder(
        Attribute("value", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
          Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("pricesNew"), None, Map(), Nil) :: Nil
      ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
        .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "value"), Order.DESC) :: Nil)
        .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
        .build()
    )
    conDef.replaceSubExpression(StringLiteral("ca"), StringLiteral("us")) should equal(
      AnonymousConceptDefinition.builder(
        Attribute("value", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
          Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
      ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("us")))
        .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "value"), Order.DESC) :: Nil)
        .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
        .build()
    )
    conDef.replaceSubExpression(FloatLiteral(10), FloatLiteral(20)) should equal(
      AnonymousConceptDefinition.builder(
        Attribute("value", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
          Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
      ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
        .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(20)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "value"), Order.DESC) :: Nil)
        .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
        .build()
    )
    conDef.replaceSubExpression(ConceptAttribute(Nil, "value"), ConceptAttribute(Nil, "category")) should equal(
      AnonymousConceptDefinition.builder(
        Attribute("value", Some(FunctionCall(FunctionReference("avg"), ConceptAttribute(Nil, "price") :: Nil)), Nil) ::
          Attribute("category", None, Nil) :: Nil,
        ParentConcept(ConceptReference("prices"), None, Map(), Nil) :: Nil
      ).attributeDependencies(Equals(ConceptAttribute(Nil, "country"), StringLiteral("ca")))
        .groupByAttributes(ConceptAttribute(Nil, "category") :: Nil)
        .groupDependencies(GreaterThan(ConceptAttribute(Nil, "price"), FloatLiteral(10)))
        .orderByAttributes(Order(ConceptAttribute(Nil, "category"), Order.DESC) :: Nil)
        .annotations(Annotation("Comment", Map("TODO" -> StringLiteral("refactor it"))) :: Nil)
        .build()
    )
    conDef.replaceSubExpression(StringLiteral("refactor it"), StringLiteral("don't touch")) should equal(
      conDef.copy(annotations = Annotation("Comment", Map("TODO" -> StringLiteral("don't touch"))) :: Nil)
    )
    assertThrows[WrongTypeException] {
      conDef.replaceSubExpression(conDef, BooleanLiteral(true))
    }


    val r = ConceptReference("prices")
    r.getSubExpressions should equal (Nil)
    r.findSubExpression(_ == NullLiteral()) should equal(None)
    r.findSubExpression(_ == r) should equal(Some(r))
    r.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    r.findAllSubExpressions(_ == r) should equal(List(r))
    r.replaceSubExpression(r, conDefToReplace) should equal(conDefToReplace)
    r.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(r)
    assertThrows[WrongTypeException] {
      r.replaceSubExpression(r, BooleanLiteral(true))
    }

    val f = AnonymousFunctionConceptDefinition(Return(Some(Variable("a"))))
    f.getSubExpressions should equal(Nil)
    f.findSubExpression(_ == NullLiteral()) should equal(None)
    f.findSubExpression(_ == f) should equal(Some(f))
    f.findSubExpression(_ == Variable("a")) should equal (Some(Variable("a")))
    f.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    f.findAllSubExpressions(_ == f) should equal(List(f))
    f.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    f.replaceSubExpression(f, conDefToReplace) should equal(conDefToReplace)
    f.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(f)
    f.replaceSubExpression(Variable("a"), Variable("b")) should equal(AnonymousFunctionConceptDefinition(Return(Some(Variable("b")))))
    assertThrows[WrongTypeException] {
      f.replaceSubExpression(f, BooleanLiteral(true))
    }
  }

  "Other expressions" should "work with subexpressions correctly" in {
    val v = Variable("a")
    v.getSubExpressions should equal(Nil)
    v.findSubExpression(_ == NullLiteral()) should equal(None)
    v.findSubExpression(_ == v) should equal(Some(v))
    v.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    v.findAllSubExpressions(_ == v) should equal(List(v))
    v.replaceSubExpression(v, IntLiteral(1)) should equal(IntLiteral(1))
    v.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(v)

    val s = Switch(Map(IntLiteral(1) -> StringLiteral("one")), StringLiteral("unknown"))
    s.getSubExpressions should equal(StringLiteral("unknown") :: IntLiteral(1)  :: StringLiteral("one") :: Nil)
    s.findSubExpression(_ == NullLiteral()) should equal(None)
    s.findSubExpression(_ == s) should equal(Some(s))
    s.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    s.findSubExpression(_ == StringLiteral("one")) should equal(Some(StringLiteral("one")))
    s.findSubExpression(_ == StringLiteral("unknown")) should equal(Some(StringLiteral("unknown")))
    s.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    s.findAllSubExpressions(_ == s) should equal(List(s))
    s.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    s.findAllSubExpressions(_ == StringLiteral("one")) should equal(List(StringLiteral("one")))
    s.findAllSubExpressions(_ == StringLiteral("unknown")) should equal(List(StringLiteral("unknown")))
    s.replaceSubExpression(s, IntLiteral(1)) should equal(IntLiteral(1))
    s.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(s)
    s.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(Switch(Map(FloatLiteral(1) -> StringLiteral("one")), StringLiteral("unknown")))
    s.replaceSubExpression(StringLiteral("one"), StringLiteral("One")) should equal(Switch(Map(IntLiteral(1) -> StringLiteral("One")), StringLiteral("unknown")))
    s.replaceSubExpression(StringLiteral("unknown"), StringLiteral("Unknown")) should equal(Switch(Map(IntLiteral(1) -> StringLiteral("one")), StringLiteral("Unknown")))

    val p = NamedElementPlaceholder("a")
    p.getSubExpressions should equal(Nil)
    p.findSubExpression(_ == NullLiteral()) should equal(None)
    p.findSubExpression(_ == p) should equal(Some(p))
    p.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    p.findAllSubExpressions(_ == p) should equal(List(p))
    p.replaceSubExpression(p, IntLiteral(1)) should equal(IntLiteral(1))
    p.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(p)

    val g = GenericParameter("a")
    g.getSubExpressions should equal(Nil)
    g.findSubExpression(_ == NullLiteral()) should equal(None)
    g.findSubExpression(_ == g) should equal(Some(g))
    g.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    g.findAllSubExpressions(_ == g) should equal(List(g))
    g.replaceSubExpression(g, IntLiteral(1)) should equal(IntLiteral(1))
    g.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(g)

    val mi = MethodInvocation("a" :: Nil, IntLiteral(1) :: Nil)
    mi.getSubExpressions should equal(IntLiteral(1) :: Nil)
    mi.findSubExpression(_ == NullLiteral()) should equal(None)
    mi.findSubExpression(_ == mi) should equal(Some(mi))
    mi.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    mi.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    mi.findAllSubExpressions(_ == mi) should equal(List(mi))
    mi.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    mi.replaceSubExpression(mi, IntLiteral(1)) should equal(IntLiteral(1))
    mi.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(mi)
    mi.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(MethodInvocation("a" :: Nil, FloatLiteral(1) :: Nil))

    val mapi = MapInitialization(Map(IntLiteral(1) -> StringLiteral("one")))
    mapi.getSubExpressions should equal(IntLiteral(1) :: StringLiteral("one") :: Nil)
    mapi.findSubExpression(_ == NullLiteral()) should equal(None)
    mapi.findSubExpression(_ == mapi) should equal(Some(mapi))
    mapi.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    mapi.findSubExpression(_ == StringLiteral("one")) should equal(Some(StringLiteral("one")))
    mapi.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    mapi.findAllSubExpressions(_ == mapi) should equal(List(mapi))
    mapi.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    mapi.findAllSubExpressions(_ == StringLiteral("one")) should equal(List(StringLiteral("one")))
    mapi.replaceSubExpression(mapi, IntLiteral(1)) should equal(IntLiteral(1))
    mapi.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(mapi)
    mapi.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(MapInitialization(Map(FloatLiteral(1) -> StringLiteral("one"))))
    mapi.replaceSubExpression(StringLiteral("one"), StringLiteral("One")) should equal(MapInitialization(Map(IntLiteral(1) -> StringLiteral("One"))))

    val listi = ListInitialization(IntLiteral(1) :: Nil)
    listi.getSubExpressions should equal(IntLiteral(1) :: Nil)
    listi.findSubExpression(_ == NullLiteral()) should equal(None)
    listi.findSubExpression(_ == listi) should equal(Some(listi))
    listi.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    listi.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    listi.findAllSubExpressions(_ == listi) should equal(List(listi))
    listi.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    listi.replaceSubExpression(listi, IntLiteral(1)) should equal(IntLiteral(1))
    listi.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(listi)
    listi.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(ListInitialization(FloatLiteral(1) :: Nil))

    val i = If(Variable("a"), IntLiteral(1), IntLiteral(2))
    i.getSubExpressions should equal(Variable("a") :: IntLiteral(1) :: IntLiteral(2) :: Nil)
    i.findSubExpression(_ == NullLiteral()) should equal(None)
    i.findSubExpression(_ == i) should equal(Some(i))
    i.findSubExpression(_ == Variable("a")) should equal(Some(Variable("a")))
    i.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    i.findSubExpression(_ == IntLiteral(2)) should equal(Some(IntLiteral(2)))
    i.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    i.findAllSubExpressions(_ == i) should equal(List(i))
    i.findAllSubExpressions(_ == Variable("a")) should equal(List(Variable("a")))
    i.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    i.findAllSubExpressions(_ == IntLiteral(2)) should equal(List(IntLiteral(2)))
    i.replaceSubExpression(i, IntLiteral(1)) should equal(IntLiteral(1))
    i.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(i)
    i.replaceSubExpression(Variable("a"), Variable("b")) should equal(If(Variable("b"), IntLiteral(1), IntLiteral(2)))
    i.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(If(Variable("a"), FloatLiteral(1), IntLiteral(2)))
    i.replaceSubExpression(IntLiteral(2), FloatLiteral(2)) should equal(If(Variable("a"), IntLiteral(1), FloatLiteral(2)))

    val fc = FunctionCall(FunctionReference("f"), IntLiteral(1) :: Nil)
    fc.getSubExpressions should equal(FunctionReference("f") :: IntLiteral(1) :: Nil)
    fc.findSubExpression(_ == NullLiteral()) should equal(None)
    fc.findSubExpression(_ == fc) should equal(Some(fc))
    fc.findSubExpression(_ == IntLiteral(1)) should equal(Some(IntLiteral(1)))
    fc.findSubExpression(_ == FunctionReference("f")) should equal(Some(FunctionReference("f")))
    fc.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    fc.findAllSubExpressions(_ == fc) should equal(List(fc))
    fc.findAllSubExpressions(_ == IntLiteral(1)) should equal(List(IntLiteral(1)))
    fc.findAllSubExpressions(_ == FunctionReference("f")) should equal(List(FunctionReference("f")))
    fc.replaceSubExpression(fc, IntLiteral(1)) should equal(IntLiteral(1))
    fc.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(fc)
    fc.replaceSubExpression(FunctionReference("f"), FunctionReference("g")) should equal(FunctionCall(FunctionReference("g"), IntLiteral(1) :: Nil))
    fc.replaceSubExpression(IntLiteral(1), FloatLiteral(1)) should equal(FunctionCall(FunctionReference("f"), FloatLiteral(1) :: Nil))
    assertThrows[WrongTypeException] {
      fc.replaceSubExpression(FunctionReference("f"), StringLiteral("f"))
    }

    val co = ConceptObject("c")
    co.getSubExpressions should equal(Nil)
    co.findSubExpression(_ == NullLiteral()) should equal(None)
    co.findSubExpression(_ == co) should equal(Some(co))
    co.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    co.findAllSubExpressions(_ == co) should equal(List(co))
    co.replaceSubExpression(co, IntLiteral(1)) should equal(IntLiteral(1))
    co.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(co)

    val ca = ConceptAttribute(Nil, "a")
    ca.getSubExpressions should equal(Nil)
    ca.findSubExpression(_ == NullLiteral()) should equal(None)
    ca.findSubExpression(_ == ca) should equal(Some(ca))
    ca.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    ca.findAllSubExpressions(_ == ca) should equal(List(ca))
    ca.replaceSubExpression(ca, IntLiteral(1)) should equal(IntLiteral(1))
    ca.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(ca)

    val ci = CollectionItem(NamedElementPlaceholder("a"), IntLiteral(0) :: Nil)
    ci.getSubExpressions should equal(NamedElementPlaceholder("a") :: IntLiteral(0) :: Nil)
    ci.findSubExpression(_ == NullLiteral()) should equal(None)
    ci.findSubExpression(_ == ci) should equal(Some(ci))
    ci.findSubExpression(_ == NamedElementPlaceholder("a")) should equal(Some(NamedElementPlaceholder("a")))
    ci.findSubExpression(_ == IntLiteral(0)) should equal(Some(IntLiteral(0)))
    ci.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    ci.findAllSubExpressions(_ == ci) should equal(List(ci))
    ci.findAllSubExpressions(_ == NamedElementPlaceholder("a")) should equal(List(NamedElementPlaceholder("a")))
    ci.findAllSubExpressions(_ == IntLiteral(0)) should equal(List(IntLiteral(0)))
    ci.replaceSubExpression(ci, IntLiteral(0)) should equal(IntLiteral(0))
    ci.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(ci)
    ci.replaceSubExpression(NamedElementPlaceholder("a"), Variable("a")) should equal (CollectionItem(Variable("a"), IntLiteral(0) :: Nil))
    ci.replaceSubExpression(IntLiteral(0), IntLiteral(1)) should equal (CollectionItem(NamedElementPlaceholder("a"), IntLiteral(1) :: Nil))
  }

  "Window functions" should "work with subexpressions correctly" in {
    val func = WindowFunction(
      WindowFunctions.SUM,
      ConceptAttribute(Nil, "a") :: Nil,
      ConceptAttribute(Nil, "b") :: Nil,
      Order(ConceptAttribute(Nil, "c"), Order.ASC) :: Nil,
      (Some(Variable("d")), Some(Variable("e"))),
      (Some(Variable("f")), Some(Variable("g")))
    )

    func.getSubExpressions should equal(
      ConceptAttribute(Nil, "a") :: ConceptAttribute(Nil, "b") :: Variable("d") :: Variable("e") :: Variable("f") :: Variable("g") :: Nil
    )
    func.findSubExpression(_ == NullLiteral()) should equal(None)
    func.findSubExpression(_ == func) should equal(Some(func))
    func.findSubExpression(_ == ConceptAttribute(Nil, "a")) should equal(Some(ConceptAttribute(Nil, "a")))
    func.findSubExpression(_ == ConceptAttribute(Nil, "b")) should equal(Some(ConceptAttribute(Nil, "b")))
    func.findSubExpression(_ == ConceptAttribute(Nil, "c")) should equal(Some(ConceptAttribute(Nil, "c")))
    func.findSubExpression(_ == Variable("d")) should equal(Some(Variable("d")))
    func.findSubExpression(_ == Variable("e")) should equal(Some(Variable("e")))
    func.findSubExpression(_ == Variable("f")) should equal(Some(Variable("f")))
    func.findSubExpression(_ == Variable("g")) should equal(Some(Variable("g")))
    func.findAllSubExpressions(_ == NullLiteral()) should equal(Nil)
    func.findAllSubExpressions(_ == func) should equal(List(func))
    func.findAllSubExpressions(_ == ConceptAttribute(Nil, "a")) should equal(List(ConceptAttribute(Nil, "a")))
    func.findAllSubExpressions(_ == ConceptAttribute(Nil, "b")) should equal(List(ConceptAttribute(Nil, "b")))
    func.findAllSubExpressions(_ == ConceptAttribute(Nil, "c")) should equal(List(ConceptAttribute(Nil, "c")))
    func.findAllSubExpressions(_ == Variable("d")) should equal(List(Variable("d")))
    func.findAllSubExpressions(_ == Variable("e")) should equal(List(Variable("e")))
    func.findAllSubExpressions(_ == Variable("f")) should equal(List(Variable("f")))
    func.findAllSubExpressions(_ == Variable("g")) should equal(List(Variable("g")))
    func.replaceSubExpression(func, IntLiteral(0)) should equal(IntLiteral(0))
    func.replaceSubExpression(BooleanLiteral(false), FloatLiteral(0)) should equal(func)
    func.replaceSubExpression(ConceptAttribute(Nil, "a"), ConceptAttribute("p" :: Nil, "a")) should equal (
      func.copy(arguments = ConceptAttribute("p" :: Nil, "a") :: Nil)
    )
    func.replaceSubExpression(ConceptAttribute(Nil, "b"), ConceptAttribute("p" :: Nil, "b")) should equal(
      func.copy(partitionBy = ConceptAttribute("p" :: Nil, "b") :: Nil)
    )
    func.replaceSubExpression(ConceptAttribute(Nil, "c"), ConceptAttribute("p" :: Nil, "c")) should equal(
      func.copy(orderBy = Order(ConceptAttribute("p" :: Nil, "c"), Order.ASC) :: Nil)
    )
    func.replaceSubExpression(Variable("d"), IntLiteral(1)) should equal(
      func.copy(rowsBetween = (Some(IntLiteral(1)), Some(Variable("e"))))
    )
    func.replaceSubExpression(Variable("e"), IntLiteral(1)) should equal(
      func.copy(rowsBetween = (Some(Variable("d")), Some(IntLiteral(1))))
    )
    func.replaceSubExpression(Variable("f"), IntLiteral(1)) should equal(
      func.copy(rangeBetween = (Some(IntLiteral(1)), Some(Variable("g"))))
    )
    func.replaceSubExpression(Variable("g"), IntLiteral(1)) should equal(
      func.copy(rangeBetween = (Some(Variable("f")), Some(IntLiteral(1))))
    )
  }


}
