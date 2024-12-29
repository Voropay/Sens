package org.sens.parser

import org.sens.core.expression
import org.sens.core.expression._
import org.sens.core.expression.concept.SensConceptExpression
import org.sens.core.expression.function.{AnonymousFunctionDefinition, FunctionReference, SensFunction}
import org.sens.core.expression.literal.{IntLiteral, NullLiteral}
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.core.statement.SensStatement

import scala.collection.mutable

abstract trait ExpressionsParser extends LiteralsParser {

  def sensExpression: Parser[SensExpression] = relationalExpression | listInitializationExpression | mapInitializationExpression |
    unaryMinusExpression | notExpression | betweenOperatorExpression | binaryOperatorExpression | windowFunctionExpression |
    ifExpression | switchExpression | functionCallExpression |
    methodInvocationExpression | anonymousFunctionDefinition | anonymousConceptDefinition | collectionItemExpression | attributeExpression |
    namedElementExpression | sensLiteral | subExpression
  def expressionOperand: Parser[SensExpression] = unaryMinusExpression | notExpression | windowFunctionExpression |  ifExpression | switchExpression |
    functionCallExpression | methodInvocationExpression | collectionItemExpression | attributeExpression | namedElementExpression |
    sensLiteral | subExpression

  def subExpression: Parser[SensExpression] = "(" ~ sensExpression ~ ")" ^^ {value => value._1._2}

  def namedElementExpression: Parser[SensExpression] = sensIdent ^^ {value => NamedElementPlaceholder(value)}

  def variableExpression: Parser[SensExpression] = sensIdent ^^ {value => Variable(value)}

  def attributeExpression: Parser[ConceptAttribute] = rep1(sensIdent ~ ".") ~ sensIdent ^^ {value => {
    val conceptNames = value._1.map(_._1)
    val attributeName = value._2
    ConceptAttribute(conceptNames, attributeName)}
  }

  def collectionItemExpression: Parser[CollectionItem] = objectOrVariableExpression ~ rep1("[" ~ sensExpression ~ "]") ^^ { value =>  {
    val collectionName = value._1
    val keyChain = value._2.map(_._1._2)
    CollectionItem(collectionName, keyChain)}
  }
  def objectOrVariableExpression = attributeExpression | namedElementExpression

  def sensFunction: Parser[SensFunction] = anonymousFunctionDefinition | functionReference
  def functionReference: Parser[SensFunction] = sensIdent ^^ {value => FunctionReference(value)}
  def anonymousFunctionDefinition: Parser[SensFunction] = "(" ~ repsep(sensIdent, ",") ~ ")" ~ "=>" ~ bodyStatement ^^ { value =>
    AnonymousFunctionDefinition(value._1._1._1._2, value._2)
  }
  def functionCallExpression: Parser[FunctionCall] = sensFunction ~ "(" ~ repsep(sensExpression, ",") <~ ")" ^^ {value => expression.FunctionCall(value._1._1, value._2)}
  def methodInvocationExpression: Parser[MethodInvocation] = attributeExpression ~ "(" ~ repsep(sensExpression, ",") ~ ")" ^^ {
    case attributes ~ "(" ~ arguments ~ ")" =>
      MethodInvocation(
        attributes.conceptsChain ::: (attributes.attribute :: Nil),
        arguments
      )
  }

  def notExpression = notKeyword ~> sensExpression ^^ {value => Not(value)}
  def unaryMinusExpression: Parser[SensExpression] = "-" ~> sensExpression ^^ {
    case IntLiteral(number) if number > 0 => IntLiteral(number * -1)
    case value => UnaryMinus(value)
  }
  def binaryOperator: Parser[String] = "+" | "-" | "*" | "/" | ">=" | ">" | "<=" | "<" | "=" | "!=" | andKeyword | orKeyword
  def binaryOperatorExpression: Parser[SensExpression] = expressionOperand ~ rep1(binaryOperator ~ expressionOperand) ^^ { values =>
    createBinaryOperator(values._1 :: values._2.map(_._2), values._2.map(_._1))
  }
  def betweenOperatorExpression: Parser[Between] = expressionOperand ~ betweenKeyword ~ "[" ~ sensExpression ~ "," ~ sensExpression ~ "]" ^^ {
    case operand ~ keyword ~ "[" ~ leftLimit ~ "," ~ rightLimit ~ "]" => Between(operand, leftLimit, rightLimit)
  }

  def listInitializationExpression: Parser[ListInitialization] =  "[" ~> repsep(sensExpression, ",") <~ "]" ^^ {value => ListInitialization(value)}
  def mapInitializationExpression: Parser[MapInitialization] = "[" ~> repsep(exprNameValuePair, ",") <~ "]" ^^ {value => MapInitialization(value.toMap)}
  def exprNameValuePair = (sensExpression ~ ":" ~ sensExpression) ^^ {case name ~ ":" ~ value => (name, value)}

  def ifExpression: Parser[If] = ifKeyword ~ sensExpression ~ thenKeyword ~ sensExpression ~ elseKeyword ~ sensExpression ^^ {
    case "if" ~ condExpr ~ "then" ~ thenExpr ~ "else" ~ elseExpr => If(condExpr, thenExpr, elseExpr)
  }

  def switchExpression: Parser[Switch] = caseKeyword ~ rep1(whenThenPair) ~ elseKeyword ~ sensExpression ^^ {
    case caseKeyword ~ cases ~ elseKeyword ~ defaultValue => Switch(cases.toMap, defaultValue)
  }

  def whenThenPair: Parser[(SensExpression, SensExpression)] = whenKeyword ~ sensExpression ~ thenKeyword ~ sensExpression ^^ {
    case whenKeyword ~ cond ~ thenKeyword ~ value => (cond, value)
  }

  def relationalExpression: Parser[SensExpression] = inListExpression | inSubQueryExpression | allExpression | anyExpression |
    existsExpression | uniqueExpression
  def inListExpression: Parser[InList] = expressionOperand ~ inKeyword ~ listInitializationExpression ^^ {
    value => InList(value._1._1, value._2)
  }
  def inSubQueryExpression: Parser[InSubQuery] = expressionOperand ~ inKeyword ~ anonymousConceptDefinition ^^ {
    value => InSubQuery(value._1._1, value._2)
  }
  def comparisonOperator: Parser[String] = ">=" | ">" | "<=" | "<" | "=" | "!="
  def allExpression: Parser[All] = expressionOperand ~ comparisonOperator ~ allKeyword ~ anonymousConceptDefinition ^^ {
    case operand1 ~ operator ~ keyword ~ conDef =>
      All(createBinaryExpression(operand1, conDef, operator).asInstanceOf[SensComparison])
  }
  def anyExpression: Parser[Any] = expressionOperand ~ comparisonOperator ~ anyKeyword ~ anonymousConceptDefinition ^^ {
    case operand1 ~ operator ~ keyword ~ conDef =>
      Any(createBinaryExpression(operand1, conDef, operator).asInstanceOf[SensComparison])
  }
  def existsExpression: Parser[Exists] = existsKeyword ~ anonymousConceptDefinition ^^ {value => Exists(value._2)}
  def uniqueExpression: Parser[Unique] = uniqueKeyword ~ anonymousConceptDefinition ^^ {value => Unique(value._2)}

  def windowFunctionExpression: Parser[WindowFunction]

  def bodyStatement: Parser[SensStatement]
  def anonymousConceptDefinition: Parser[SensConceptExpression]

  val priorities = Map("not" -> 0, "*" -> 1, "/" -> 1, "+" -> 2, "-" -> 2, ">" -> 3, "<" -> 3, ">=" -> 3, "<=" -> 3, "=" -> 4, "!=" -> 4, "and" -> 5, "or" -> 5)
  def createBinaryOperator(terms: List[SensExpression], operations: List[String]): SensExpression = {
    val indices = operations.zipWithIndex
    val ordered = indices.sortWith(
      (item1, item2) => {
        val priority1 = priorities.getOrElse(item1._1, 5)
        val priority2 = priorities.getOrElse(item2._1, 5)
        priority1 <= priority2
      }
    )
    val currentOperations = new mutable.ListBuffer ++ indices
    val currentTerms = new mutable.ListBuffer ++ terms
    ordered.foreach(curOp => {
      val curIndex = currentOperations.indexOf(curOp)
      val newOp = createBinaryExpression(currentTerms(curIndex), currentTerms(curIndex + 1), curOp._1 )
      currentOperations.remove(curIndex)
      currentTerms.remove(curIndex, 2)
      currentTerms.insert(curIndex, newOp)
    })
    currentTerms(0)
  }

  def createBinaryExpression(term1: SensExpression, term2: SensExpression, operation: String): SensExpression = {
    operation match {
      case "+" => Add(term1, term2)
      case "-" => Substract(term1, term2)
      case "*" => Multiply(term1, term2)
      case "/" => Divide(term1, term2)
      case ">" => GreaterThan(term1, term2)
      case "<" => LessThan(term1, term2)
      case ">=" => GreaterOrEqualsThan(term1, term2)
      case "<=" => LessOrEqualsThan(term1, term2)
      case "=" => Equals(term1, term2)
      case "!=" => NotEquals(term1, term2)
      case "and" => And(term1, term2)
      case "or" => Or(term1, term2)
    }
  }
}
