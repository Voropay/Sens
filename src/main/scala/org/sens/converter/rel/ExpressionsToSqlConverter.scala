package org.sens.converter.rel

import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.apache.calcite.sql.fun.{SqlCase, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlBasicCall, SqlBasicTypeNameSpec, SqlCall, SqlDataTypeSpec, SqlFunction, SqlFunctionCategory, SqlIdentifier, SqlIntervalQualifier, SqlKind, SqlLiteral, SqlNode, SqlNodeList, SqlNumericLiteral, SqlSelectKeyword, SqlWindow}
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder}
import org.apache.commons.lang.NotImplementedException
import org.sens.core.concept.Order
import org.sens.core.expression.concept.AnonymousConceptDefinition
import org.sens.core.expression.{CollectionItem, ConceptAttribute, ConceptObject, FunctionCall, If, ListInitialization, MapInitialization, SensExpression, Switch, WindowFunction, WindowFunctions}
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.literal.SensBasicTypes.SensBasicType
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.relational._
import org.sens.parser.{ElementNotFoundException, ValidationContext, WrongArgumentsException, WrongArgumentsTypeException}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class ExpressionsToSqlConverter(context: ValidationContext, config: FrameworkConfig, conceptConverter: ConceptToSqlConverter) {

  val sqlOperatorMap = StandardSensFunctions.standardSensFunctions ++ config.getOperatorTable.getOperatorList.asScala.map(op =>
    (op.getName, op)
  ).toMap

  def sensExpressionToSql(expression: SensExpression): String = toSql(sensExpressionToSqlNode(expression))

  def sensExpressionToSqlNode(expression: SensExpression): SqlNode = {
    expression match {
      case FunctionCall(reference: FunctionReference, arguments) =>
        if (sqlOperatorMap.contains(reference.name)) {
          createFunctionCall(reference.name, arguments)
        } else {
          throw ElementNotFoundException(reference.name)
        }

      case ConceptAttribute(conceptsChain, attribute) => new SqlIdentifier((conceptsChain ::: List(attribute)).asJava, SqlParserPos.ZERO)
      case co: ConceptObject => {
        val attributes = co.getAttributeNames(context).map(ConceptAttribute(co.name :: Nil, _)).map(sensExpressionToSqlNode(_))
        new SqlBasicCall(
          SqlStdOperatorTable.ROW,
          attributes.asJava,
          SqlParserPos.ZERO)
      }

      case And(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.AND,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case AndSeq(exprSeq) =>
        composeConjunction(exprSeq.map(sensExpressionToSqlNode))
      case Or(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.OR,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case Not(expr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.NOT,
          (sensExpressionToSqlNode(expr) :: Nil).asJava,
          SqlParserPos.ZERO)

      case Add(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.PLUS,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case Substract(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.MINUS,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case Multiply(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.MULTIPLY,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case Divide(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.DIVIDE,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case UnaryMinus(expr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.UNARY_MINUS,
          (sensExpressionToSqlNode(expr) :: Nil).asJava,
          SqlParserPos.ZERO)

      case Equals(leftExpr, rightExpr) =>
        if (rightExpr.isInstanceOf[NullLiteral]) {
          new SqlBasicCall(
            SqlStdOperatorTable.IS_NULL,
            (sensExpressionToSqlNode(leftExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        } else if (leftExpr.isInstanceOf[NullLiteral]) {
          new SqlBasicCall(
            SqlStdOperatorTable.IS_NULL,
            (sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        } else {
          new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        }
      case NotEquals(leftExpr, rightExpr) =>
        if (rightExpr.isInstanceOf[NullLiteral]) {
          new SqlBasicCall(
            SqlStdOperatorTable.IS_NOT_NULL,
            (sensExpressionToSqlNode(leftExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        } else if (leftExpr.isInstanceOf[NullLiteral]) {
          new SqlBasicCall(
            SqlStdOperatorTable.IS_NOT_NULL,
            (sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        } else {
          new SqlBasicCall(
            SqlStdOperatorTable.NOT_EQUALS,
            (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
            SqlParserPos.ZERO)
        }
      case GreaterThan(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.GREATER_THAN,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case GreaterOrEqualsThan(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case LessThan(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.LESS_THAN,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case LessOrEqualsThan(leftExpr, rightExpr) =>
        new SqlBasicCall(
          SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          (sensExpressionToSqlNode(leftExpr) :: sensExpressionToSqlNode(rightExpr) :: Nil).asJava,
          SqlParserPos.ZERO)
      case Between(operand, leftLimit, rightLimit) =>
        new SqlBasicCall(
          SqlStdOperatorTable.BETWEEN,
          (sensExpressionToSqlNode(operand) :: sensExpressionToSqlNode(leftLimit) :: sensExpressionToSqlNode(rightLimit) :: Nil).asJava,
          SqlParserPos.ZERO)

      case IntLiteral(value) => SqlLiteral.createExactNumeric(value.toString, SqlParserPos.ZERO)
      case FloatLiteral(value) => SqlLiteral.createExactNumeric(value.toString, SqlParserPos.ZERO)
      case StringLiteral(value) => SqlLiteral.createCharString(value, SqlParserPos.ZERO)
      case BooleanLiteral(value) => SqlLiteral.createBoolean(value, SqlParserPos.ZERO)
      case NullLiteral() => SqlLiteral.createNull(SqlParserPos.ZERO)
      case BasicTypeLiteral(basicType) =>
        val typeName: SqlTypeName = ExpressionsToSqlConverter.getTypeByName(basicType)
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO), SqlParserPos.ZERO)
      case ListLiteral(items) =>
        new SqlBasicCall(
          SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
          items.map(sensExpressionToSqlNode).asJava,
          SqlParserPos.ZERO)
      case ListInitialization(items) =>
        new SqlBasicCall(
          SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
          items.map(sensExpressionToSqlNode).asJava,
          SqlParserPos.ZERO)
      case MapLiteral(items) =>
        val operands = items.flatMap(item =>
          sensExpressionToSqlNode(item._1) ::
            sensExpressionToSqlNode(item._2) :: Nil
        ).toList.asJava
        new SqlBasicCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
          operands,
          SqlParserPos.ZERO)
      case MapInitialization(items) =>
        val operands = items.flatMap(item =>
          sensExpressionToSqlNode(item._1) ::
            sensExpressionToSqlNode(item._2) :: Nil
        ).toList.asJava
        new SqlBasicCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
          operands,
          SqlParserPos.ZERO)
      case interval: TimeIntervalLiteral =>
        val intervalQualifier = new SqlIntervalQualifier(
          ExpressionsToRelConverter.toCalciteTimeUnit(interval.getLeadingTimeUnit),
          interval.getLeadingPrecision,
          ExpressionsToRelConverter.toCalciteTimeUnit(interval.getTrailingTimeUnit),
          interval.getTrailingPrecision,
          SqlParserPos.ZERO
        )
        SqlLiteral.createInterval(1, interval.getValue.toString, intervalQualifier, SqlParserPos.ZERO)

      case ci: CollectionItem => {
        val normalizedCollectionItem = ci.toCollectionItemChain
        val collectionName = sensExpressionToSqlNode(normalizedCollectionItem.collectionName)
        val keysChain = sensExpressionToSqlNode(normalizedCollectionItem.keysChain.head)
        new SqlBasicCall(
          SqlStdOperatorTable.ITEM,
          (collectionName :: keysChain :: Nil).asJava,
          SqlParserPos.ZERO)
      }

      case Switch(conditions, defaultValue) => {
        val whenList = conditions.keys.map(sensExpressionToSqlNode).toList.asJava
        val thenList = conditions.values.map(sensExpressionToSqlNode).toList.asJava
        val elseExpr = sensExpressionToSqlNode(defaultValue)
        new SqlCase(
          SqlParserPos.ZERO,
          null,
          new SqlNodeList(whenList, SqlParserPos.ZERO),
          new SqlNodeList(thenList, SqlParserPos.ZERO),
          elseExpr)
      }
      case If(condition, thenExpr, elseExpr) => {
        val whenList = (sensExpressionToSqlNode(condition) :: Nil).asJava
        val thenList = (sensExpressionToSqlNode(thenExpr) :: Nil).asJava
        val elseNode = sensExpressionToSqlNode(elseExpr)
        new SqlCase(
          SqlParserPos.ZERO,
          null,
          new SqlNodeList(whenList, SqlParserPos.ZERO),
          new SqlNodeList(thenList, SqlParserPos.ZERO),
          elseNode)
      }

      case inList: InList => {
        val operand = sensExpressionToSqlNode(inList.operand1)
        val values = new SqlNodeList(inList.operand2.items.map(sensExpressionToSqlNode).asJava, SqlParserPos.ZERO)
        new SqlBasicCall(
          SqlStdOperatorTable.IN,
          (operand :: values :: Nil).asJava,
          SqlParserPos.ZERO)
      }
      case inSubQuery: InSubQuery => {
        val operand = sensExpressionToSqlNode(inSubQuery.operand1)
        val conDef = inSubQuery.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val subQueryNode = conceptConverter.toSqlNode(conDef)
        new SqlBasicCall(
          SqlStdOperatorTable.IN,
          (operand :: subQueryNode :: Nil).asJava,
          SqlParserPos.ZERO)
      }
      case all: All => {
        val operand1 = sensExpressionToSqlNode(all.operand.operand1)
        val operatorKind = getComparisonOperatorByName(all.operand.name)
        val conDef = all.operand.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case x => throw WrongArgumentsTypeException(All.getClass.getName, AnonymousConceptDefinition.getClass.getName, x.getClass.getName)
        }
        val subQueryNode = conceptConverter.toSqlNode(conDef)
        new SqlBasicCall(
          SqlStdOperatorTable.all(operatorKind),
          (operand1 :: subQueryNode :: Nil).asJava,
          SqlParserPos.ZERO)
      }
      case any: Any => {
        val operand1 = sensExpressionToSqlNode(any.operand.operand1)
        val operatorKind = getComparisonOperatorByName(any.operand.name)
        val conDef = any.operand.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case x => throw WrongArgumentsTypeException(All.getClass.getName, AnonymousConceptDefinition.getClass.getName, x.getClass.getName)
        }
        val subQueryNode = conceptConverter.toSqlNode(conDef)
        new SqlBasicCall(
          SqlStdOperatorTable.some(operatorKind),
          (operand1 :: subQueryNode :: Nil).asJava,
          SqlParserPos.ZERO)
      }
      case exists: Exists => {
        val conDef = exists.operand match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val subQueryNode = conceptConverter.prepareQuery(conDef)
        val existsOp = new SqlFunction(
          "EXISTS",
          SqlKind.EXISTS,
          ReturnTypes.BOOLEAN,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.SYSTEM
        )
        SqlStdOperatorTable.EXISTS.createCall(SqlParserPos.ZERO, subQueryNode)
      }
      case unique: Unique => {
        val conDef = unique.operand match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val subQueryNode = conceptConverter.prepareQuery(conDef)
        val uniqueOp = new SqlFunction(
          "UNIQUE",
          SqlKind.UNIQUE,
          ReturnTypes.BOOLEAN,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.SYSTEM
        )
        SqlStdOperatorTable.UNIQUE.createCall(SqlParserPos.ZERO, subQueryNode)
      }

      case wf: WindowFunction => {
        val windowFunctionCall = getWindowFunctionCallByName(wf)

        val partitionList: SqlNodeList = new SqlNodeList(wf.partitionBy.map(sensExpressionToSqlNode).asJava, SqlParserPos.ZERO)
        val orderList: SqlNodeList = new SqlNodeList(wf.orderBy.map(curOrder => {
          val node = sensExpressionToSqlNode(curOrder.attribute)
          if (curOrder.direction == Order.ASC) {
            node
          } else {
            new SqlBasicCall(
              SqlStdOperatorTable.DESC,
              List(node).asJava,
              SqlParserPos.ZERO)
          }
        }).asJava, SqlParserPos.ZERO)
        val isRows = if(wf.rowsBetween._1.isDefined || wf.rowsBetween._2.isDefined) {
          SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
        } else {
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
        }
        val lowerBoundValue = wf.rowsBetween._1.orElse(wf.rangeBetween._1)
        val upperBoundValue = wf.rowsBetween._2.orElse(wf.rangeBetween._2)

        val lowerBound = if(lowerBoundValue.isDefined) {
          if(lowerBoundValue.get == IntLiteral(0)) {
            SqlWindow.createCurrentRow(SqlParserPos.ZERO)
          } else {
            SqlWindow.createPreceding(sensExpressionToSqlNode(lowerBoundValue.get), SqlParserPos.ZERO)
          }
        } else if(upperBoundValue.isDefined) {
          SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO)
        } else {
          null
        }

        val upperBound = if (upperBoundValue.isDefined) {
          if (upperBoundValue.get == IntLiteral(0)) {
            SqlWindow.createCurrentRow(SqlParserPos.ZERO)
          } else {
            SqlWindow.createFollowing(sensExpressionToSqlNode(upperBoundValue.get), SqlParserPos.ZERO)
          }
        } else {
          null
        }
        val window = new SqlWindow(
          SqlParserPos.ZERO,
          null,
          null,
          partitionList,
          orderList,
          isRows,
          lowerBound,
          upperBound,
          null
        )

        SqlStdOperatorTable.OVER.createCall(SqlParserPos.ZERO, windowFunctionCall, window)
      }


      case _ => throw new NotImplementedException
    }
  }

  def getComparisonOperatorByName(name: String): SqlKind = {
    name match {
      case ">" => SqlKind.GREATER_THAN
      case ">=" => SqlKind.GREATER_THAN_OR_EQUAL
      case "<" => SqlKind.LESS_THAN
      case "<=" => SqlKind.LESS_THAN_OR_EQUAL
      case "=" => SqlKind.EQUALS
      case "!=" => SqlKind.NOT_EQUALS

    }
  }

  def getWindowFunctionCallByName(wf: WindowFunction): SqlCall = {
    wf.functionName match {
      case WindowFunctions.SUM => createFunctionCall("sum", wf.arguments)
      case WindowFunctions.AVG => createFunctionCall("avg", wf.arguments)
      case WindowFunctions.COUNT => createFunctionCall("count", wf.arguments)
      case WindowFunctions.MIN => createFunctionCall("min", wf.arguments)
      case WindowFunctions.MAX => createFunctionCall("max", wf.arguments)
      case WindowFunctions.ROW_NUMBER => SqlStdOperatorTable.ROW_NUMBER.createCall(SqlParserPos.ZERO)
      case WindowFunctions.RANK => SqlStdOperatorTable.RANK.createCall(SqlParserPos.ZERO)
      case WindowFunctions.DENSE_RANK => SqlStdOperatorTable.DENSE_RANK.createCall(SqlParserPos.ZERO)
      case WindowFunctions.PERCENT_RANK => SqlStdOperatorTable.PERCENT_RANK.createCall(SqlParserPos.ZERO)
      case WindowFunctions.CUME_DIST => SqlStdOperatorTable.CUME_DIST.createCall(SqlParserPos.ZERO)
      case WindowFunctions.LEAD => {
        val expr = sensExpressionToSqlNode(wf.arguments.head)
        val offset = if (wf.arguments.size > 1) {
          sensExpressionToSqlNode(wf.arguments(1))
        } else {
          SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
        }
        val defaultVal = if (wf.arguments.size > 2) {
          sensExpressionToSqlNode(wf.arguments(2))
        } else {
          SqlLiteral.createNull(SqlParserPos.ZERO)
        }
        SqlStdOperatorTable.LEAD.createCall(SqlParserPos.ZERO, List(expr, offset, defaultVal).asJava)
      }
      case WindowFunctions.LAG => {
        val expr = sensExpressionToSqlNode(wf.arguments.head)
        val offset = if (wf.arguments.size > 1) {
          sensExpressionToSqlNode(wf.arguments(1))
        } else {
          SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
        }
        val defaultVal = if (wf.arguments.size > 2) {
          sensExpressionToSqlNode(wf.arguments(2))
        } else {
          SqlLiteral.createNull(SqlParserPos.ZERO)
        }
        SqlStdOperatorTable.LAG.createCall(SqlParserPos.ZERO, List(expr, offset, defaultVal).asJava)
      }
      case WindowFunctions.NTILE =>
        SqlStdOperatorTable.NTILE.createCall(SqlParserPos.ZERO, List(sensExpressionToSqlNode(wf.arguments.head)).asJava)
      case WindowFunctions.FIRST_VALUE =>
        SqlStdOperatorTable.FIRST_VALUE.createCall(SqlParserPos.ZERO, List(sensExpressionToSqlNode(wf.arguments.head)).asJava)
      case WindowFunctions.LAST_VALUE =>
        SqlStdOperatorTable.LAST_VALUE.createCall(SqlParserPos.ZERO, List(sensExpressionToSqlNode(wf.arguments.head)).asJava)
      case WindowFunctions.NTH_VALUE =>
        SqlStdOperatorTable.NTH_VALUE.createCall(
          SqlParserPos.ZERO,
          List(sensExpressionToSqlNode(wf.arguments.head), sensExpressionToSqlNode(wf.arguments(1))).asJava
      )
      case _ => throw new NotImplementedException
    }
  }

  def createFunctionCall(functionName: String, arguments: List[SensExpression]): SqlCall = {
    val sqlOperator = sqlOperatorMap(functionName)
    if (StandardSensFunctions.standardSensAggregateFunctions.contains(functionName)
      && arguments.nonEmpty && arguments.head == StringLiteral("DISTINCT")) {
      sqlOperator.createCall(SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO), SqlParserPos.ZERO, arguments.tail.map(sensExpressionToSqlNode).asJava)
    }
    else {
      new SqlBasicCall(sqlOperator, arguments.map(sensExpressionToSqlNode).asJava, SqlParserPos.ZERO)
    }
  }

  def composeConjunction(operandSeq: List[SqlNode]): SqlNode = {
    if (operandSeq.isEmpty) {
      throw WrongArgumentsException("AndSeq", 0)
    } else if (operandSeq.size == 1) {
      operandSeq.head
    } else if (operandSeq.size == 2) {
      new SqlBasicCall(
        SqlStdOperatorTable.AND,
        operandSeq.asJava,
        SqlParserPos.ZERO)
    } else {
      val operandsTailNode = composeConjunction(operandSeq.tail)
      new SqlBasicCall(
        SqlStdOperatorTable.AND,
        List(operandSeq.head, operandsTailNode).asJava,
        SqlParserPos.ZERO)
    }
  }



  def toSql(sqlNode: SqlNode): String = {
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }

}

object ExpressionsToSqlConverter {
  def create(context: ValidationContext, conceptConverter: ConceptToSqlConverter): ExpressionsToSqlConverter = {
    val (config, _) = ConceptToRelConverter.prepareSchemaAndConfig(context)
    new ExpressionsToSqlConverter(context, config, conceptConverter)
  }

  def getTypeByName(basicType: SensBasicType): SqlTypeName = {
    basicType match {
      case SensBasicTypes.BOOLEAN_TYPE => SqlTypeName.BOOLEAN
      case SensBasicTypes.FLOAT_TYPE => SqlTypeName.FLOAT
      case SensBasicTypes.INT_TYPE => SqlTypeName.INTEGER
      case SensBasicTypes.STRING_TYPE => SqlTypeName.VARCHAR
    }
  }
}
