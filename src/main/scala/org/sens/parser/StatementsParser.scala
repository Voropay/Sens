package org.sens.parser

import org.sens.core.concept.SensConcept
import org.sens.core.expression.Variable
import org.sens.core.statement._

trait StatementsParser extends ExpressionsParser {

  def sensStatement: Parser[SensStatement] = variableDefinitionStatement | variableAssignmentStatement | addToMapStatement | addToListStatement |
    returnStatement | breakStatement | continueStatement | incrementStatement | decrementStatement | ifStatement |
    forStatement | foreachItemInListStatement | foreachItemInMapStatement | whileStatement | doWhileStatement |
    procedureCallStatement | functionDefinitionStatement | conceptDefinitionStatement | compositeStatement

  override def bodyStatement: Parser[SensStatement] = compositeStatement | sensStatement

  def compositeStatement: Parser[StatementBlock] = "{" ~ repsep(sensStatement, rep1(statementsSeparator)) ~ rep(statementsSeparator) ~ "}" ^^ { value => StatementBlock(value._1._1._2)}
  def statementsSeparator: Parser[String] = ";"

  def returnStatement = returnKeyword ~ opt(sensExpression) ^^ { value => Return(value._2)}
  def breakStatement = breakKeyword ^^ { _ => Break()}
  def continueStatement = continueKeyword ^^ { _ => Continue()}

  def variableAssignmentStatement: Parser[VariableAssignment] = sensIdent ~ "=" ~ sensExpression ^^ {value => VariableAssignment(value._1._1, value._2)}
  def variableDefinitionStatement: Parser[SensStatement] = varKeyword ~> sensIdent ~ opt("=" ~> sensExpression) ^^ {value =>
    VariableDefinition(value._1, value._2)
  }

  def ifStatement: Parser[If] = ifKeyword ~ sensExpression ~ thenKeyword ~ bodyStatement ~ opt(elseKeyword ~> bodyStatement) ^^ {
    case "if" ~ cond ~ "then" ~ thenStmt ~ elseStmt => If(cond, thenStmt, elseStmt)
  }

  def incrementStatement: Parser[Increment] = variableExpression <~ "++" ^^ {value => Increment(value)}
  def decrementStatement: Parser[Decrement] = variableExpression <~ "--" ^^ {value => Decrement(value)}

  def procedureCallStatement: Parser[ProcedureCall] = functionCallExpression ^^ {value => ProcedureCall(value)}

  def forStatement: Parser[For] = forKeyword ~ "(" ~ opt(bodyStatement) ~ ";" ~ opt(sensExpression) ~ ";" ~ opt(bodyStatement) ~ ")" ~(bodyStatement) ^^ {
    case "for" ~ "(" ~ startStmt ~ ";" ~ exitCond ~ ";" ~ endStmt ~ ")" ~ loopBody => For(
      startStmt,
      exitCond,
      endStmt,
      loopBody
    )
  }

  def foreachItemInListStatement: Parser[ForEachItemInList] = forKeyword ~ "(" ~ sensIdent ~ inKeyword ~ sensExpression ~ ")" ~ bodyStatement ^^ {
    case "for" ~ "(" ~ iteratorName ~ "in" ~ collection ~ ")" ~ loopBody => ForEachItemInList(iteratorName, collection, loopBody)
  }

  def foreachItemInMapStatement: Parser[ForEachItemInMap] = forKeyword ~ "(" ~ sensIdent ~ "," ~ sensIdent ~ inKeyword ~ sensExpression ~ ")" ~ bodyStatement ^^ {
    case "for" ~ "(" ~ iteratorKey ~ "," ~ iteratorValue ~ "in" ~ collection ~ ")" ~ loopBody => ForEachItemInMap(iteratorKey, iteratorValue, collection, loopBody)
  }

  def whileStatement: Parser[While] = whileKeyword ~ sensExpression ~ doKeyword ~ bodyStatement ^^ {
    case "while" ~ exitCond ~ "do" ~ loopBody => While(exitCond, loopBody)
  }

  def doWhileStatement: Parser[DoWhile] = doKeyword ~ bodyStatement ~ whileKeyword ~ sensExpression ^^ {
    case "do" ~ loopBody ~ "while" ~ exitCond => DoWhile(exitCond, loopBody)
  }

  def addToListStatement: Parser[AddToList] = sensIdent ~ "[]" ~ "=" ~ sensExpression ^^ {
    case collection ~ "[]" ~ "=" ~ value => AddToList(Variable(collection), value)
  }

  def addToMapStatement: Parser[AddToMap] = sensIdent ~ "[" ~ sensExpression ~ "]" ~ "=" ~ sensExpression ^^ {
    case collection ~ "[" ~ key ~ "]" ~ "=" ~ value => AddToMap(Variable(collection), key, value)
  }

  def functionDefinitionStatement: Parser[FunctionDefinition] = defKeyword ~ sensIdent ~ "(" ~ repsep(sensIdent, ",") ~ ")" ~ bodyStatement ^^ {
    case "def" ~ functionName ~ "(" ~ argList ~ ")" ~ body => FunctionDefinition(functionName, argList, body)
  }

  def conceptDefinitionStatement: Parser[ConceptDefinition] = sensConcept ^^ {value => ConceptDefinition(value)}

  def sensConcept: Parser[SensConcept]


}
