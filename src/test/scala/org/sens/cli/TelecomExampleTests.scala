package org.sens.cli

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.sens.core.expression.literal.{ListLiteral, StringLiteral}
import org.sens.parser.SensParser

class TelecomExampleTests extends AnyFlatSpec with Matchers {
  val source = scala.io.Source.fromFile("examples/telecom.sens")
  val program = try source.mkString finally source.close()

  "Telecom Program" should "be parsed and validated correctly" in {
    val conceptParser = new SensParser
    val parsingResults = MainApp.parseDataModel(program, conceptParser)
    parsingResults.isRight should be(true)
    val context = parsingResults.right.get
    context.getConcepts.size should equal(15)

    val sqlCode = MainApp.materialize(context, false, Some(ListLiteral(StringLiteral("2024-01-01") :: Nil)))
    sqlCode.contains("internetUsageMetrics") should be (true)
    sqlCode.contains("callsMetrics") should be (true)
    sqlCode.contains("cellConnectionMetrics") should be (true)
    sqlCode.contains("customerBehaviorMetrics") should be (true)
    //println(sqlCode)
  }
}
