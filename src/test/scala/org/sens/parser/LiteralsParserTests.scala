package org.sens.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression.literal.{BasicTypeLiteral, BooleanLiteral, FloatLiteral, IntLiteral, ListLiteral, MapLiteral, NullLiteral, SensBasicTypes, SensLiteral, StringLiteral}

class LiteralsParserTests extends AnyFlatSpec with Matchers {

  val literalsParser = new SensParser {
    def apply(code: String): Option[SensLiteral] = {
      parse(sensLiteral, code) match {
        case Success(res, _) => Some(res)
        case _ => None
      }
    }
  }

  "Basic Literals" should "be parsed correctly" in {

    literalsParser("Null").get should equal (NullLiteral())
    literalsParser("true").get should equal (BooleanLiteral(true))
    literalsParser("false").get should equal (BooleanLiteral(false))
    literalsParser("10.5").get should equal (FloatLiteral(10.5))
    literalsParser("10.0").get should equal (FloatLiteral(10.0))
    literalsParser("-0.5").get should equal (FloatLiteral(-0.5))
    literalsParser("0.0").get should equal (FloatLiteral(0))
    literalsParser("10").get should equal (IntLiteral(10))
    literalsParser("-10").get should equal (IntLiteral(-10))
    literalsParser("0").get should equal (IntLiteral(0))
    literalsParser("\"str\"").get should equal (StringLiteral("str"))
    literalsParser("\"\"").get should equal (StringLiteral(""))
    literalsParser("int").get should equal (BasicTypeLiteral(SensBasicTypes.INT_TYPE))
    literalsParser("float").get should equal (BasicTypeLiteral(SensBasicTypes.FLOAT_TYPE))
    literalsParser("boolean").get should equal (BasicTypeLiteral(SensBasicTypes.BOOLEAN_TYPE))
    literalsParser("string").get should equal (BasicTypeLiteral(SensBasicTypes.STRING_TYPE))

  }

  "List Literal" should "be parsed correctly" in {
    literalsParser("[1, \"str\", false]").get should equal (
      ListLiteral(IntLiteral(1) :: StringLiteral("str") :: BooleanLiteral(false) :: Nil)
    )
    literalsParser("[]").get should equal (ListLiteral(Nil))
    literalsParser("[1, [2, [3]]]").get should equal (
      ListLiteral(
        IntLiteral(1) ::
          ListLiteral(
            IntLiteral(2) ::
              ListLiteral(
                IntLiteral(3) ::
                  Nil) ::
              Nil
          ) ::
          Nil
      ))
  }

  "Map Literal" should "be parsed correctly" in {
    literalsParser("[1: \"one\", 2: \"two\", 3: \"three\"]").get should equal (
      MapLiteral(Map(
        IntLiteral(1) -> StringLiteral("one"),
        IntLiteral(2) -> StringLiteral("two"),
        IntLiteral(3) -> StringLiteral("three")
      ))
    )

    literalsParser("[\"a\": \"one\", \"b\": [\"two\", \"three\"], \"c\": [4: \"four\", 5: \"five\"]]").get should equal (
      MapLiteral(Map(
        StringLiteral("a") -> StringLiteral("one"),
        StringLiteral("b") -> ListLiteral(StringLiteral("two") :: StringLiteral("three") :: Nil),
        StringLiteral("c") -> MapLiteral(Map(
          IntLiteral(4) -> StringLiteral("four"),
          IntLiteral(5) -> StringLiteral("five")
        ))
      ))
    )
  }
}
