package org.sens.converter.rel

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rex.{RexNode, RexWindowBounds}
import org.apache.calcite.sql.{SqlBinaryOperator, SqlIntervalQualifier}
import org.apache.calcite.sql.`type`.{SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder}
import org.apache.calcite.tools.RelBuilder.{AggCall, OverCall}
import org.apache.commons.lang.NotImplementedException
import org.sens.core.concept.{Attribute, Order}
import org.sens.core.expression.concept.AnonymousConceptDefinition
import org.sens.core.expression.function._
import org.sens.core.expression.literal._
import org.sens.core.expression.operation.arithmetic._
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression._
import org.sens.core.expression.operation.logical._
import org.sens.core.expression.operation.relational._
import org.sens.parser.{AttributeExpressionNotFound, ElementNotFoundException, ValidationContext, ValidationException, WrongArgumentsTypeException, WrongFunctionArgumentsException}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class ExpressionsToRelConverter(config: FrameworkConfig, context: ValidationContext, conceptConverter: ConceptToRelConverter) {

  val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)

  val sqlOperatorMap = StandardSensFunctions.standardSensFunctions ++ config.getOperatorTable.getOperatorList.asScala.map(op =>
    (op.getName, op)
  ).toMap

  def sensExpressionToRexNode(expression: SensExpression, childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int): RexNode = {
    expression match {

      //Function
      case FunctionCall(reference: FunctionReference, arguments) => {
        if(sqlOperatorMap.contains(reference.name)) {
          val sqlOperator = sqlOperatorMap(reference.name)
          if(sqlOperator == SqlStdOperatorTable.CAST) {
            val arg = sensExpressionToRexNode(arguments.head, childAttributes, relBuilder, numberOfTables)
            arguments.tail.head match {
              case BooleanTypeLiteral() =>  relBuilder.cast(arg, SqlTypeName.BOOLEAN)
              case FloatTypeLiteral() =>  relBuilder.cast(arg, SqlTypeName.FLOAT)
              case IntTypeLiteral() =>  relBuilder.cast(arg, SqlTypeName.INTEGER)
              case StringTypeLiteral(length) =>  relBuilder.cast(arg, SqlTypeName.VARCHAR, length)
              case ListTypeLiteral(_) => throw new NotImplementedException
              case MapTypeLiteral(_) => throw new NotImplementedException
              case other => throw WrongArgumentsTypeException("cast", "SensTypeLiteral", other.getClass.getName)
            }
          } else {
            relBuilder.call(sqlOperator, arguments.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables)).asJava)
          }
        } else {
          throw ElementNotFoundException(reference.name)
        }
      }

      //Concept Attributes
      case ConceptAttribute(conceptsChain, attribute) => {
        if(conceptsChain.isEmpty) {
          //If there is no attribute expressions return field alias from the top of the node stack
          if(childAttributes.isEmpty) {
            return relBuilder.field(attribute)
          }
          //Replace child attribute with its value
          val attributeExpressionOpt = childAttributes.find(_.name == attribute)
          if(attributeExpressionOpt.isEmpty || attributeExpressionOpt.get.value.isEmpty) {
            throw AttributeExpressionNotFound(attribute)
          }
          sensExpressionToRexNode(
            attributeExpressionOpt.get.value.get,
            childAttributes,
            relBuilder,
            numberOfTables)
        } else {
          relBuilder.field(numberOfTables, conceptsChain.head, attribute)
        }
      }

      //Logical operations
      case And(leftExpr, rightExpr) => relBuilder.and(
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case AndSeq(exprSeq) => relBuilder.and(
        exprSeq.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables)).asJava
      )
      case Or(leftExpr, rightExpr) => relBuilder.or(
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case Not(expr) => relBuilder.not(
        sensExpressionToRexNode(expr, childAttributes, relBuilder, numberOfTables))

      //Arithmetic operations
      case Add(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.PLUS,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case Substract(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.MINUS,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case Multiply(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.MULTIPLY,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case Divide(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.DIVIDE,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case UnaryMinus(expr) => relBuilder.call(SqlStdOperatorTable.UNARY_MINUS,
        sensExpressionToRexNode(expr, childAttributes, relBuilder, numberOfTables))

      //Comparison operations
      case Equals(leftExpr, rightExpr) =>
        if(rightExpr.isInstanceOf[NullLiteral]) {
          relBuilder.isNull(sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables))
        } else if(leftExpr.isInstanceOf[NullLiteral]) {
          relBuilder.isNull(sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
        } else {
          relBuilder.call(SqlStdOperatorTable.EQUALS,
          sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
          sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
        }
      case NotEquals(leftExpr, rightExpr) =>
        if(rightExpr.isInstanceOf[NullLiteral]) {
          relBuilder.isNotNull(sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables))
        } else if(leftExpr.isInstanceOf[NullLiteral]) {
          relBuilder.isNotNull(sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
        } else {
          relBuilder.call(SqlStdOperatorTable.NOT_EQUALS,
          sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
          sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
        }
      case GreaterThan(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case LessThan(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.LESS_THAN,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case GreaterOrEqualsThan(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case LessOrEqualsThan(leftExpr, rightExpr) => relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        sensExpressionToRexNode(leftExpr, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightExpr, childAttributes, relBuilder, numberOfTables))
      case Between(operand, leftLimit, rightLimit) => relBuilder.call(SqlStdOperatorTable.BETWEEN,
        sensExpressionToRexNode(operand, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(leftLimit, childAttributes, relBuilder, numberOfTables),
        sensExpressionToRexNode(rightLimit, childAttributes, relBuilder, numberOfTables))

      //Literals
      case IntLiteral(value) => relBuilder.literal(value)
      case FloatLiteral(value) => relBuilder.literal(value)
      case StringLiteral(value) => relBuilder.literal(value)
      case BooleanLiteral(value) => relBuilder.literal(value)
      case NullLiteral() => relBuilder.literal(null)
      case ListLiteral(items) => {
        val operands = items.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables)).asJava
        relBuilder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands)
      }
      case ListInitialization(items) => {
        val operands = items.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables)).asJava
        relBuilder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands)
      }
      case MapLiteral(items) => {
        val operands = items.flatMap(item =>
          sensExpressionToRexNode(item._1, childAttributes, relBuilder, numberOfTables) ::
            sensExpressionToRexNode(item._2, childAttributes, relBuilder, numberOfTables) :: Nil
        ).toList.asJava
        relBuilder.call(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, operands)
      }
      case MapInitialization(items) => {
        val operands = items.flatMap(item =>
          sensExpressionToRexNode(item._1, childAttributes, relBuilder, numberOfTables) ::
            sensExpressionToRexNode(item._2, childAttributes, relBuilder, numberOfTables) :: Nil
        ).toList.asJava
        relBuilder.call(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, operands)
      }
      case interval: TimeIntervalLiteral => {
        val intervalQualifier = new SqlIntervalQualifier(
          ExpressionsToRelConverter.toCalciteTimeUnit(interval.getLeadingTimeUnit),
          interval.getLeadingPrecision,
          ExpressionsToRelConverter.toCalciteTimeUnit(interval.getTrailingTimeUnit),
          interval.getTrailingPrecision,
          SqlParserPos.ZERO
        )
        val intervalLiteral = relBuilder.getRexBuilder.makeIntervalLiteral(
          java.math.BigDecimal.valueOf(interval.getValue),
          intervalQualifier
        )
        intervalLiteral
      }

      //Conditional operators
      case Switch(conditions, defaultValue) => {
        val arguments = conditions.flatMap(item =>
          sensExpressionToRexNode(item._1, childAttributes, relBuilder, numberOfTables) ::
            sensExpressionToRexNode(item._2, childAttributes, relBuilder, numberOfTables) :: Nil
        ).toList ::: List(sensExpressionToRexNode(defaultValue, childAttributes, relBuilder, numberOfTables))
        relBuilder.call(SqlStdOperatorTable.CASE, arguments.asJava)
      }
      case If(condition, thenExpr, elseExpr) => relBuilder.call(
        SqlStdOperatorTable.CASE,
        sensExpressionToRexNode(condition, childAttributes, relBuilder, numberOfTables),
          sensExpressionToRexNode(thenExpr, childAttributes, relBuilder, numberOfTables),
          sensExpressionToRexNode(elseExpr, childAttributes, relBuilder, numberOfTables)
      )
      //Collection item
      case ci: CollectionItem => {
        val normalizedCollectionItem = ci.toCollectionItemChain
        relBuilder.call(
          SqlStdOperatorTable.ITEM,
          sensExpressionToRexNode(normalizedCollectionItem.collectionName, childAttributes, relBuilder, numberOfTables),
          sensExpressionToRexNode(normalizedCollectionItem.keysChain.head, childAttributes, relBuilder, numberOfTables)
        )
      }
      case co: ConceptObject => {
        val attributes = co.getAttributeNames(context).map(ConceptAttribute(co.name :: Nil, _))
        val attributeRexNodes = attributes.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables))
        relBuilder.call(
          SqlStdOperatorTable.ROW,
          attributeRexNodes.asJava
        )
      }

      case inList: InList => {
        val arg = sensExpressionToRexNode(inList.operand1, childAttributes, relBuilder, numberOfTables)
        val argList: List[RexNode] = inList.operand2.items.map(sensExpressionToRexNode(_, childAttributes, relBuilder, numberOfTables))
        relBuilder.in(arg, argList.asJava)
      }

      case inSubQuery: InSubQuery => {
        val arg = sensExpressionToRexNode(inSubQuery.operand1, childAttributes, relBuilder, numberOfTables)
        val conDef = inSubQuery.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val f = new java.util.function.Function[RelBuilder, RelNode] {
          override def apply(relBuilder: RelBuilder): RelNode = conceptConverter.toRel(conDef, relBuilder)
        }
        relBuilder.in(arg, f)
      }

      case all: All => {
        val operand1 = sensExpressionToRexNode(all.operand.operand1, childAttributes, relBuilder, numberOfTables)
        val operator = getComparisonOperatorByName(all.operand.name)
        val conDef = all.operand.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case x => throw WrongArgumentsTypeException(All.getClass.getName, AnonymousConceptDefinition.getClass.getName, x.getClass.getName)
        }
        val operand2 = new java.util.function.Function[RelBuilder, RelNode] {
          override def apply(relBuilder: RelBuilder): RelNode = conceptConverter.toRel(conDef, relBuilder)
        }
        relBuilder.all(operand1, operator, operand2)
      }

      case any: Any => {
        val operand1 = sensExpressionToRexNode(any.operand.operand1, childAttributes, relBuilder, numberOfTables)
        val operator = getComparisonOperatorByName(any.operand.name)
        val conDef = any.operand.operand2 match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case x => throw WrongArgumentsTypeException(All.getClass.getName, AnonymousConceptDefinition.getClass.getName, x.getClass.getName)
        }
        val operand2 = new java.util.function.Function[RelBuilder, RelNode] {
          override def apply(relBuilder: RelBuilder): RelNode = conceptConverter.toRel(conDef, relBuilder, numberOfTables + 1)
        }
        relBuilder.some(operand1, operator, operand2)
      }

      case exists: Exists => {
        val conDef = exists.operand match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val operand = new java.util.function.Function[RelBuilder, RelNode] {
          override def apply(relBuilder: RelBuilder): RelNode = conceptConverter.toRel(conDef, relBuilder, numberOfTables + 1)
        }
        relBuilder.exists(operand)
      }

      case unique: Unique => {
        val conDef = unique.operand match {
          case acd: AnonymousConceptDefinition => acd.toConcept()
          case _ => throw new NotImplementedException
        }
        val operand = new java.util.function.Function[RelBuilder, RelNode] {
          override def apply(relBuilder: RelBuilder): RelNode = conceptConverter.toRel(conDef, relBuilder)
        }
        relBuilder.unique(operand)
      }

      case wf: WindowFunction => {
        val aggCall = wf.functionName match {
          case WindowFunctions.SUM => relBuilder.sum(sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.AVG => relBuilder.avg(sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.COUNT =>
            if(wf.arguments.size == 1) {
              relBuilder.count(sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
            } else {
              relBuilder.count()
            }
          case WindowFunctions.MIN => relBuilder.min(sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.MAX => relBuilder.max(sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.ROW_NUMBER => relBuilder.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
          case WindowFunctions.RANK => relBuilder.aggregateCall(SqlStdOperatorTable.RANK)
          case WindowFunctions.DENSE_RANK => relBuilder.aggregateCall(SqlStdOperatorTable.DENSE_RANK)
          case WindowFunctions.PERCENT_RANK => relBuilder.aggregateCall(SqlStdOperatorTable.PERCENT_RANK)
          case WindowFunctions.CUME_DIST => relBuilder.aggregateCall(SqlStdOperatorTable.CUME_DIST)
          case WindowFunctions.LEAD => {
            val expr = sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables)
            val offset = if(wf.arguments.size > 1) {
              sensExpressionToRexNode(wf.arguments(1), Nil, relBuilder, numberOfTables)
            } else {
              relBuilder.literal(1)
            }
            val defaultVal = if (wf.arguments.size > 2) {
              sensExpressionToRexNode(wf.arguments(2), Nil, relBuilder, numberOfTables)
            } else {
              relBuilder.literal(null)
            }
            relBuilder.aggregateCall(SqlStdOperatorTable.LEAD, expr, offset, defaultVal)
          }
          case WindowFunctions.LAG => {
            val expr = sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables)
            val offset = if (wf.arguments.size > 1) {
              sensExpressionToRexNode(wf.arguments(1), Nil, relBuilder, numberOfTables)
            } else {
              relBuilder.literal(1)
            }
            val defaultVal = if (wf.arguments.size > 2) {
              sensExpressionToRexNode(wf.arguments(2), Nil, relBuilder, numberOfTables)
            } else {
              relBuilder.literal(null)
            }
            relBuilder.aggregateCall(SqlStdOperatorTable.LAG, expr, offset, defaultVal)
          }
          case WindowFunctions.NTILE => relBuilder.aggregateCall(
            SqlStdOperatorTable.NTILE,
            sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.FIRST_VALUE => relBuilder.aggregateCall(
            SqlStdOperatorTable.FIRST_VALUE,
            sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.LAST_VALUE => relBuilder.aggregateCall(
            SqlStdOperatorTable.LAST_VALUE,
            sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables))
          case WindowFunctions.NTH_VALUE => relBuilder.aggregateCall(
            SqlStdOperatorTable.NTH_VALUE,
            sensExpressionToRexNode(wf.arguments.head, Nil, relBuilder, numberOfTables),
            sensExpressionToRexNode(wf.arguments(1), Nil, relBuilder, numberOfTables)
          )
          case _ => throw new NotImplementedException
        }
        val overCall = aggCall.over()
        val overCallPartitioned: OverCall =  if(wf.partitionBy.nonEmpty) {
          overCall.partitionBy(wf.partitionBy.map(sensExpressionToRexNode(_, Nil, relBuilder, numberOfTables)).asJava)
        } else {
          overCall
        }

        val overCallOrdered: OverCall = if(wf.orderBy.nonEmpty) {
          val orderArgs: List[RexNode] = wf.orderBy.map(expr => {
            val node = sensExpressionToRexNode(expr.attribute, Nil, relBuilder, numberOfTables)
            if (expr.direction == Order.DESC) {
              relBuilder.desc(node)
            } else {
              node
            }
          })
          overCallPartitioned.orderBy(orderArgs.asJava)
        } else {
          overCallPartitioned
        }

        val overCallRows = if(wf.rowsBetween._1.isDefined && wf.rowsBetween._2.isDefined) {
          overCallOrdered.rowsBetween(
            RexWindowBounds.preceding(sensExpressionToRexNode(wf.rowsBetween._1.get, Nil, relBuilder, numberOfTables)),
            RexWindowBounds.following(sensExpressionToRexNode(wf.rowsBetween._2.get, Nil, relBuilder, numberOfTables))
          )
        } else if(wf.rowsBetween._1.isDefined) {
          overCallOrdered.rowsFrom(RexWindowBounds.preceding(sensExpressionToRexNode(wf.rowsBetween._1.get, Nil, relBuilder, numberOfTables)))
        } else if(wf.rowsBetween._2.isDefined) {
          overCallOrdered.rowsTo(RexWindowBounds.preceding(sensExpressionToRexNode(wf.rowsBetween._2.get, Nil, relBuilder, numberOfTables)))
        } else {
          overCallOrdered
        }

        val overCallRange = if (wf.rangeBetween._1.isDefined && wf.rangeBetween._2.isDefined) {
          overCallRows.rangeBetween(
            RexWindowBounds.preceding(sensExpressionToRexNode(wf.rangeBetween._1.get, Nil, relBuilder, numberOfTables)),
            RexWindowBounds.following(sensExpressionToRexNode(wf.rangeBetween._2.get, Nil, relBuilder, numberOfTables))
          )
        } else if (wf.rangeBetween._1.isDefined) {
          overCallRows.rangeFrom(RexWindowBounds.preceding(sensExpressionToRexNode(wf.rangeBetween._1.get, Nil, relBuilder, numberOfTables)))
        } else if (wf.rangeBetween._2.isDefined) {
          overCallRows.rangeTo(RexWindowBounds.preceding(sensExpressionToRexNode(wf.rangeBetween._2.get, Nil, relBuilder, numberOfTables)))
        } else {
          overCallRows
        }
        overCallRange.toRex
      }

      case _ => throw new NotImplementedException
    }
  }

  def attributeToAggregateCall(attribute: Attribute, relBuilder: RelBuilder, numberOfTables: Int): AggCall = {
    if(attribute.value.isEmpty) {
      throw AttributeExpressionNotFound(attribute.name)
    }
    attribute.value.get match {
      case FunctionCall(definition, arguments) => prepareAggregateCall(attribute.name, definition, arguments, relBuilder, numberOfTables)
      case _ => throw new ValidationException(
        "Aggregate expression expected instead of %s for attribute %s"
          .format(attribute.value.get.toSensString, attribute.name)
      )
    }
  }

  def prepareAggregateCall(
                           alias: String,
                           definition: SensFunction,
                           arguments: List[SensExpression],
                           relBuilder: RelBuilder,
                           numberOfTables: Int): AggCall = {
    definition match {
      case _: AnonymousFunctionDefinition => throw new NotImplementedException
      case r: FunctionReference => {
        val aggArgs = getAggregateFunctionArguments(arguments)
        r.name match {
          case "count" => if(arguments.isEmpty) {
            relBuilder.count(false, alias)
          } else {
            relBuilder.count(true, alias, sensExpressionToRexNode(arguments.head, Nil, relBuilder, numberOfTables))
          }
          case "sum" => relBuilder.sum(aggArgs._1, alias, sensExpressionToRexNode(aggArgs._2, Nil, relBuilder, numberOfTables))
          case "avg" => relBuilder.avg(aggArgs._1, alias, sensExpressionToRexNode(aggArgs._2, Nil, relBuilder, numberOfTables))
          case "min" => relBuilder.min(alias, sensExpressionToRexNode(arguments.head, Nil, relBuilder, numberOfTables))
          case "max" => relBuilder.max(alias, sensExpressionToRexNode(arguments.head, Nil, relBuilder, numberOfTables))
        }
      }
    }
  }

  def getAggregateFunctionArguments(arguments: List[SensExpression]): (Boolean, SensExpression) = {
    if(arguments.size == 1) {
      (false, arguments.head)
    } else if(arguments.isEmpty) {
      (false, null)
    }
    else {
      (arguments.head.asInstanceOf[BooleanLiteral].value, arguments.tail.head)
    }
  }

  def getComparisonOperatorByName(name: String): SqlBinaryOperator = {
    name match {
      case ">" => SqlStdOperatorTable.GREATER_THAN
      case ">=" => SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
      case "<" => SqlStdOperatorTable.LESS_THAN
      case "<=" => SqlStdOperatorTable.LESS_THAN_OR_EQUAL
      case "=" => SqlStdOperatorTable.EQUALS
      case "!=" => SqlStdOperatorTable.NOT_EQUALS

    }
  }
}

object ExpressionsToRelConverter {
  def toCalciteTimeUnit(unit: TimeIntervalLiteral.TimeUnit): TimeUnit = {
    if (unit == null) {
      return null
    }
    unit match {
      case TimeIntervalLiteral.YEAR => TimeUnit.YEAR
      case TimeIntervalLiteral.MONTH => TimeUnit.MONTH
      case TimeIntervalLiteral.DAY => TimeUnit.DAY
      case TimeIntervalLiteral.HOUR => TimeUnit.HOUR
      case TimeIntervalLiteral.MINUTE => TimeUnit.MINUTE
      case TimeIntervalLiteral.SECOND => TimeUnit.SECOND
    }
  }
}
