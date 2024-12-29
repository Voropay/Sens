package org.sens.parser

import org.sens.core.DataModelElement
import org.sens.core.expression.SensExpression
import org.sens.core.expression.concept.AnonymousConceptDefinition

case class DataModelParserError(msg: String)

class SensParser extends ConceptParser {

  def dataModelElement: Parser[DataModelElement] = functionDefinitionStatement | conceptDefinitionStatement
  def dataModelParser: Parser[List[DataModelElement]] = phrase(repsep(dataModelElement, rep1(statementsSeparator)) ~ rep(statementsSeparator)) ^^
      { value => value._1}

  def parseDataModel(code: String): Either[DataModelParserError, List[DataModelElement]] = {
    parse(dataModelParser, code) match {
      case NoSuccess(msg, _) => Left(DataModelParserError(msg))
      case Success(result, _) => Right(result)
    }
  }

  def parseQuery(code: String): Either[DataModelParserError, AnonymousConceptDefinition] = {
    parse(anonymousConceptExpression, "<" + code + ">") match {
      case NoSuccess(msg, _) => Left(DataModelParserError(msg))
      case Success(result, _) => Right(result)
    }
  }

  def parseExpression(code: String): Either[DataModelParserError, SensExpression] = {
    parse(sensExpression, code) match {
      case NoSuccess(msg, _) => Left(DataModelParserError(msg))
      case Success(result, _) => Right(result)
    }
  }
}
