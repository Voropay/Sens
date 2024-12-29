package org.sens.optimiser

import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.Variable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.converter.optimization.expression._
import org.sens.core.expression.literal.BooleanLiteral

class ExpressionRuleTests extends AnyFlatSpec with Matchers {
  "And operator" should "correctly be converted to AndSeq" in {
    AndSeqRule(
      And(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal (
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )

    AndSeqRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    AndSeqRule(
      And(AndSeq(Variable("a") :: Variable("b") :: Nil), And(Variable("c"), Variable("d")))
    ) should equal(
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )

    AndSeqRule(
      And(Variable("a"), And(Variable("b"), And(Variable("c"), Variable("d"))))
    ) should equal(
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )

    AndSeqRule(
      And(And(And(Variable("a"), Variable("b")), Variable("c")), Variable("d"))
    ) should equal(
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )

  }

  "AndSeq nested And operands" should "correctly be assembled to AndSeq" in {
    AndSeqRule(
      AndSeq(AndSeq(Variable("a") :: Variable("b") :: Nil) :: AndSeq(Variable("c") :: Variable("d") :: Nil) :: Nil)
    ) should equal(
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )

    AndSeqRule(
      AndSeq(And(Variable("a"), Variable("b")) :: And(Variable("c"), Variable("d")) :: Nil)
    ) should equal(
      AndSeq(List(Variable("a"), Variable("b"), Variable("c"), Variable("d")))
    )
  }

  "Not(A Or B) expression" should "correctly be converted to AndSeq(Not(A), Not(B))" in {
    NotOrRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    NotOrRule(
      And(Not(Or(Variable("a"), Variable("b"))), Variable("c"))
    ) should equal(
      And(AndSeq(Not(Variable("a")) :: Not(Variable("b")) :: Nil), Variable("c"))
    )
  }

  "Not(Not(A) expression" should "correctly be converted to A" in {
    NotNotRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    NotNotRule(
      And(Not(Not(Variable("a"))), Not(Not(Variable("b"))))
    ) should equal(
      And(Variable("a"), Variable("b"))
    )
  }

  "And(true, A) expressions" should "correctly be converted to A" in {
    AndTrueRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    AndTrueRule(
      And(BooleanLiteral(true), Variable("a"))
    ) should equal(
      Variable("a")
    )

    AndTrueRule(
      And(Variable("a"), BooleanLiteral(true))
    ) should equal(
      Variable("a")
    )

    AndTrueRule(
      And(BooleanLiteral(true), BooleanLiteral(true))
    ) should equal(
      BooleanLiteral(true)
    )

    AndTrueRule(
      And(BooleanLiteral(true), And(BooleanLiteral(true), Variable("a")))
    ) should equal(
      Variable("a")
    )

    AndTrueRule(
      AndSeq(BooleanLiteral(true) :: BooleanLiteral(true) :: Variable("a") :: Nil)
    ) should equal(
      Variable("a")
    )

    AndTrueRule(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Variable("b") :: Nil)
    ) should equal(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Variable("b") :: Nil)
    )

    AndTrueRule(
      AndSeq(BooleanLiteral(true) :: BooleanLiteral(true) :: BooleanLiteral(true) :: Nil)
    ) should equal(
      BooleanLiteral(true)
    )

    AndTrueRule(
      AndSeq(BooleanLiteral(true) :: BooleanLiteral(true) :: And(BooleanLiteral(true), Variable("a")) :: Nil)
    ) should equal(
      Variable("a")
    )
  }

  "And(false, A) expressions" should "correctly be converted to false" in {
    AndFalseRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    AndFalseRule(
      And(BooleanLiteral(false), Variable("a"))
    ) should equal(
      BooleanLiteral(false)
    )

    AndFalseRule(
      And(Variable("a"), BooleanLiteral(false))
    ) should equal(
      BooleanLiteral(false)
    )

    AndFalseRule(
      And(BooleanLiteral(false), BooleanLiteral(false))
    ) should equal(
      BooleanLiteral(false)
    )

    AndFalseRule(
      And(BooleanLiteral(true), And(BooleanLiteral(false), Variable("a")))
    ) should equal(
      BooleanLiteral(false)
    )

    AndFalseRule(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Variable("b") :: Nil)
    ) should equal(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Variable("b") :: Nil)
    )

    AndFalseRule(
      AndSeq(BooleanLiteral(true) :: BooleanLiteral(true) :: And(BooleanLiteral(false), Variable("a")) :: Nil)
    ) should equal(
      BooleanLiteral(false)
    )
  }

  "Or(true, A) expressions" should "correctly be converted to true" in {
    OrTrueRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    OrTrueRule(
      Or(BooleanLiteral(true), Variable("a"))
    ) should equal(
      BooleanLiteral(true)
    )

    OrTrueRule(
      Or(Variable("a"), BooleanLiteral(true))
    ) should equal(
      BooleanLiteral(true)
    )

    OrTrueRule(
      Or(BooleanLiteral(true), BooleanLiteral(true))
    ) should equal(
      BooleanLiteral(true)
    )

    OrTrueRule(
      Or(BooleanLiteral(true), Or(BooleanLiteral(true), Variable("a")))
    ) should equal(
      BooleanLiteral(true)
    )
  }

  "Or(false, A) expressions" should "correctly be converted to A" in {
    OrFalseRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    OrFalseRule(
      Or(BooleanLiteral(false), Variable("a"))
    ) should equal(
      Variable("a")
    )

    OrFalseRule(
      Or(Variable("a"), BooleanLiteral(false))
    ) should equal(
      Variable("a")
    )

    OrFalseRule(
      Or(BooleanLiteral(false), BooleanLiteral(false))
    ) should equal(
      BooleanLiteral(false)
    )

    OrFalseRule(
      Or(BooleanLiteral(false), Or(BooleanLiteral(false), Variable("a")))
    ) should equal(
      Variable("a")
    )
  }

  "And(A, Not(A)) expressions" should "correctly be converted to false" in {
    AndANotARule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    AndANotARule(
      And(Variable("a"), Not(Variable("a")))
    ) should equal(
      BooleanLiteral(false)
    )

    AndANotARule(
      And(Not(Variable("a")), Variable("a"))
    ) should equal(
      BooleanLiteral(false)
    )

    AndANotARule(
      Or(And(Variable("a"), Not(Variable("a"))), And(Not(Variable("b")), Variable("b")))
    ) should equal(
      Or(BooleanLiteral(false), BooleanLiteral(false))
    )

    AndANotARule(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Not(Variable("a")) :: Nil)
    ) should equal(
      BooleanLiteral(false)
    )

    AndANotARule(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Not(Variable("b")) :: Nil)
    ) should equal(
      AndSeq(BooleanLiteral(true) :: Variable("a") :: Not(Variable("b")) :: Nil)
    )
  }

  "Or(A, Not(A)) expressions" should "correctly be converted to true" in {
    OrANotARule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    OrANotARule(
      Or(Variable("a"), Not(Variable("a")))
    ) should equal(
      BooleanLiteral(true)
    )

    OrANotARule(
      Or(Not(Variable("a")), Variable("a"))
    ) should equal(
      BooleanLiteral(true)
    )

    OrANotARule(
      And(Or(Variable("a"), Not(Variable("a"))), Or(Not(Variable("b")), Variable("b")))
    ) should equal(
      And(BooleanLiteral(true), BooleanLiteral(true))
    )
  }

  "Not(Equals(A, B)) expression" should "correctly be converted to NotEquals(A, B)" in {
    NotEqualsRule(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    ) should equal(
      Or(And(Variable("a"), Variable("b")), And(Variable("c"), Variable("d")))
    )

    NotEqualsRule(
      And(Not(Equals(Variable("a"), Variable("b"))), Equals(Variable("c"), Variable("d")))
    ) should equal(
      And(NotEquals(Variable("a"), Variable("b")), Equals(Variable("c"), Variable("d")))
    )
  }
}
