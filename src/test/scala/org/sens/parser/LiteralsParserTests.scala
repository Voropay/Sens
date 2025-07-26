package org.sens.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression.literal._

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

  "Type Literals" should "be parsed correctly" in {
    literalsParser("int").get should equal(IntTypeLiteral())
    literalsParser("float").get should equal(FloatTypeLiteral())
    literalsParser("boolean").get should equal(BooleanTypeLiteral())
    literalsParser("string(255)").get should equal(StringTypeLiteral(255))
    literalsParser("list<string(255)>").get should equal(ListTypeLiteral(StringTypeLiteral(255)))
    literalsParser("map<string(255) attr1, int attr2>").get should equal(
      MapTypeLiteral(Map("attr1" -> StringTypeLiteral(255), "attr2" -> IntTypeLiteral()))
    )
    literalsParser("map<string(255) attr1, map<list<int> attr21, list<boolean> attr22> attr2>").get should equal(
      MapTypeLiteral(Map(
        "attr1" -> StringTypeLiteral(255),
        "attr2" -> MapTypeLiteral(Map(
          "attr21" -> ListTypeLiteral(IntTypeLiteral()),
          "attr22" -> ListTypeLiteral(BooleanTypeLiteral())
        ))))
    )
  }
}
