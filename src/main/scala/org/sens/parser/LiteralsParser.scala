package org.sens.parser

import org.sens.core.expression.literal._

import scala.util.parsing.combinator.JavaTokenParsers

trait LiteralsParser extends JavaTokenParsers {
  def sensLiteral: Parser[SensLiteral] = listExpression | mapExpression | sensNullLiteral | sensBooleanLiteral | sensFloatLiteral |
    sensIntLiteral | sensStringLiteral | sensTypeLiteral
  def sensBooleanLiteral: Parser[BooleanLiteral] = (trueKeyword | falseKeyword) ^^ {value => BooleanLiteral(value == "true")}
  def sensIntLiteral: Parser[IntLiteral] = wholeNumber ^^  {value => IntLiteral(value.toInt)}
  def sensFloatLiteral: Parser[SensLiteral] = floatingPointNumber ^^  {value => {
    val floatingValue = value.toDouble
    if(value.contains(".") || floatingValue != floatingValue.toInt.toDouble) {
      FloatLiteral(floatingValue)
    } else {
      IntLiteral(floatingValue.toInt)
    }
  }}
  def sensTypeLiteral: Parser[BasicTypeLiteral] = intTypeLiteral | floatTypeLiteral | stringTypeLiteral | booleanTypeLiteral
  def intTypeLiteral: Parser[BasicTypeLiteral] = intKeyword ^^ {_ => BasicTypeLiteral(SensBasicTypes.INT_TYPE)}
  def floatTypeLiteral: Parser[BasicTypeLiteral] = floatKeyword ^^ {_ => BasicTypeLiteral(SensBasicTypes.FLOAT_TYPE)}
  def stringTypeLiteral: Parser[BasicTypeLiteral] = stringKeyword ^^ {_ => BasicTypeLiteral(SensBasicTypes.STRING_TYPE)}
  def booleanTypeLiteral: Parser[BasicTypeLiteral] = booleanKeyword ^^ {_ => BasicTypeLiteral(SensBasicTypes.BOOLEAN_TYPE)}

  def sensStringLiteral: Parser[StringLiteral] = stringLiteral ^^ {value => StringLiteral(removeQuotes(value))}
  def sensNullLiteral: Parser[NullLiteral] = nullKeyword ^^ {_ => NullLiteral()}

  def listExpression: Parser[ListLiteral] =  "[" ~> repsep(sensLiteral, ",") <~ "]" ^^ {value => ListLiteral(value)}
  def mapExpression: Parser[MapLiteral] = "[" ~> repsep(nameValuePair, ",") <~ "]" ^^ {value => MapLiteral(value.toMap)}
  def nameValuePair = (sensLiteral ~ ":" ~ sensLiteral) ^^ {case name ~ ":" ~ value => (name, value)}
  def removeQuotes(value: String): String = value.substring(1, value.size - 1)

  def sensIdent: Parser[String] = not(reserved) ~> ident
  def reserved: Parser[String] = nullKeyword | trueKeyword | falseKeyword | notKeyword | andKeyword | orKeyword |
    returnKeyword | breakKeyword | continueKeyword | varKeyword |
    ifKeyword | thenKeyword | elseKeyword |
    forKeyword | inKeyword | whileKeyword | doKeyword | defKeyword |
    allKeyword | anyKeyword | existsKeyword | uniqueKeyword |
    conceptKeyword | conceptCubeKeyword | dataSourceKeyword | fromKeyword | isKeyword | betweenKeyword | byKeyword |
    unionKeyword | intersectKeyword | minusKeyword |
    whereKeyword | withKeyword | withoutKeyword | orderByKeyword |
    groupByKeyword | havingKeyword | limitKeyword | offsetKeyword |
    intKeyword | floatKeyword | booleanKeyword | stringKeyword

  def trueKeyword: Parser[String] = "true\\b".r
  def falseKeyword: Parser[String] = "false\\b".r
  def nullKeyword: Parser[String] = "Null\\b".r
  def notKeyword: Parser[String] = "not\\b".r
  def andKeyword: Parser[String] = "and\\b".r
  def orKeyword: Parser[String] = "or\\b".r

  def returnKeyword: Parser[String] = "return\\b".r
  def breakKeyword: Parser[String] = "break\\b".r
  def continueKeyword: Parser[String] = "continue\\b".r
  def varKeyword: Parser[String] = "var\\b".r
  def ifKeyword: Parser[String] = "if\\b".r
  def thenKeyword: Parser[String] = "then\\b".r
  def elseKeyword: Parser[String] = "else\\b".r
  def caseKeyword: Parser[String] = "case\\b".r
  def whenKeyword: Parser[String] = "when\\b".r
  def forKeyword: Parser[String] = "for\\b".r
  def inKeyword: Parser[String] = "in\\b".r
  def whileKeyword: Parser[String] = "while\\b".r
  def doKeyword: Parser[String] = "do\\b".r
  def defKeyword: Parser[String] = "def\\b".r

  def conceptKeyword: Parser[String] = "concept\\b".r
  def conceptCubeKeyword: Parser[String] = "concept\\scube\\b".r
  def dataSourceKeyword: Parser[String] = "datasource\\b".r
  def fromKeyword: Parser[String] = "from\\b".r
  def isKeyword: Parser[String] = "is\\b".r
  def betweenKeyword: Parser[String] = "between\\b".r
  def byKeyword: Parser[String] = "by\\b".r
  def unionKeyword: Parser[String] = "union\\sof\\b".r
  def intersectKeyword: Parser[String] = "intersect\\sof\\b".r
  def minusKeyword: Parser[String] = "minus\\b".r

  def whereKeyword: Parser[String] = "where\\b".r
  def withKeyword: Parser[String] = "with\\b".r
  def withoutKeyword: Parser[String] = "without\\b".r
  def metricsKeyword: Parser[String] = "metrics\\b".r
  def dimensionsKeyword: Parser[String] = "dimensions\\b".r
  def orderByKeyword: Parser[String] = "order\\sby\\b".r
  def orderAscKeyword: Parser[String] = "asc\\b".r
  def orderDescKeyword: Parser[String] = "desc\\b".r
  def groupByKeyword: Parser[String] = "group\\sby\\b".r
  def havingKeyword: Parser[String] = "having\\b".r
  def limitKeyword: Parser[String] = "limit\\b".r
  def offsetKeyword: Parser[String] = "offset\\b".r

  def anyKeyword: Parser[String] = "any\\b".r
  def allKeyword: Parser[String] = "all\\b".r
  def existsKeyword: Parser[String] = "exists\\b".r
  def uniqueKeyword: Parser[String] = "unique\\b".r

  def overKeyword: Parser[String] = "over\\b".r
  def partitionByKeyword: Parser[String] = "partition\\sby\\b".r
  def rowsBetweenKeyword: Parser[String] = "rows\\sbetween\\b".r
  def rangeBetweenKeyword: Parser[String] = "range\\sbetween\\b".r

  def intKeyword: Parser[String] = "int\\b".r
  def floatKeyword: Parser[String] = "float\\b".r
  def stringKeyword: Parser[String] = "string\\b".r
  def booleanKeyword: Parser[String] = "boolean\\b".r

}