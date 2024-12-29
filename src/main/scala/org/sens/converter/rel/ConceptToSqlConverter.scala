package org.sens.converter.rel

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{JoinConditionType, JoinType, SqlBasicCall, SqlCall, SqlIdentifier, SqlJoin, SqlLiteral, SqlNode, SqlNodeList, SqlSelect, SqlSelectKeyword, SqlSetOperator}
import org.apache.calcite.tools.FrameworkConfig
import org.sens.core.concept.{AggregationConcept, Annotation, Attribute, Concept, CubeConcept, CubeInheritedConcept, DataSourceConcept, FunctionConcept, InheritedConcept, IntersectConcept, MinusConcept, Order, ParentConcept, SensConcept, UnionConcept}
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference, SensConceptExpression}
import org.sens.parser.{AttributeExpressionNotFound, ElementNotFoundException, ValidationContext, WrongArgumentsException}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class ConceptToSqlConverter (context: ValidationContext, config: FrameworkConfig) {
  val validationContext = context.addFrame
  val expressionConverter = new ExpressionsToSqlConverter(validationContext, config, this)

  def toSql(concept: SensConcept): String = {
    val query = toSqlNode(concept)
    expressionConverter.toSql(query)
  }

  def toSqlNode(concept: SensConcept): SqlNode = {
    validationContext.setCurrentConcept(concept)
    val query = concept match {
      case conceptDef: Concept => {
        prepareQuery(conceptDef.inferAttributeExpressions(context).get)
      }
      case cubeConcept: CubeConcept => {
        val conceptDef = cubeConcept.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef)
      }
      case cubeInheritedConcept: CubeInheritedConcept => {
        val conceptDef = cubeInheritedConcept.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef)
      }
      case inheritedDef: InheritedConcept => {
        val conceptDef = inheritedDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef)
      }
      case aggregationDef: AggregationConcept => {
        val conceptDef = aggregationDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef)
      }
      case dsDef: DataSourceConcept => {
        prepareDataSourceQuery(dsDef)
      }
      case intersectConceptDef: IntersectConcept => {
        val parentConceptsSql = intersectConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConceptQuery(pc.concept, pc.alias))
        composeSetQuery(parentConceptsSql, SqlStdOperatorTable.INTERSECT)
      }
      case unionConceptDef: UnionConcept => {
        val parentConceptsSql = unionConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConceptQuery(pc.concept, pc.alias))
        composeSetQuery(parentConceptsSql, SqlStdOperatorTable.UNION)
      }
      case minusConceptDef: MinusConcept => {
        val parentConceptsSql = minusConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConceptQuery(pc.concept, pc.alias))
        composeSetQuery(parentConceptsSql, SqlStdOperatorTable.EXCEPT)
      }
      case _ =>
        throw new NotImplementedError()
    }
    query
  }

  def composeSetQuery(queries: List[SqlNode], operator: SqlSetOperator): SqlNode = {
    if (queries.isEmpty) {
      throw WrongArgumentsException("parent concepts", 0)
    } else if(queries.size == 1) {
      queries.head
    } else {
      val firstPair = new SqlBasicCall(
        operator,
        (queries(0) :: queries(1) :: Nil).asJava,
        SqlParserPos.ZERO)
      queries.tail.tail.fold(firstPair)((acc, nextEl) =>
        new SqlBasicCall(
          operator,
          (acc :: nextEl :: Nil).asJava,
          SqlParserPos.ZERO)
      )
    }
  }

  def prepareQuery(concept: Concept): SqlSelect = {
    val keywords = if(concept.annotations.contains(Annotation.UNIQUE)) {
      new SqlNodeList(List(SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO)).asJava, SqlParserPos.ZERO)
    } else {
      null
    }
    val selectList: SqlNodeList = attributesToSqlNodeList(concept.attributes)
    val joins: SqlNode = parentConceptsToJoin(concept.parentConcepts)
    val where = if(concept.attributeDependencies.isEmpty) {
      null
    } else {
      expressionConverter.sensExpressionToSqlNode(concept.attributeDependencies.get)
    }
    val groupBy = if(concept.groupByAttributes.isEmpty) {
      null
    } else {
      new SqlNodeList(concept.groupByAttributes.map(expressionConverter.sensExpressionToSqlNode).asJava, SqlParserPos.ZERO)
    }
    val having = if(concept.groupDependencies.isEmpty) {
      null
    } else {
      expressionConverter.sensExpressionToSqlNode(concept.groupDependencies.get)
    }
    val windowDecls = null
    val orderBy = if (concept.orderByAttributes.isEmpty) {
      null
    } else {
      new SqlNodeList(concept.orderByAttributes.map(orderAttr => {
        val expr = expressionConverter.sensExpressionToSqlNode(orderAttr.attribute)
        if(orderAttr.direction == Order.ASC) {
          expr
        } else {
          new SqlBasicCall(
            SqlStdOperatorTable.DESC,
            List(expr).asJava,
            SqlParserPos.ZERO)
        }
      }).asJava, SqlParserPos.ZERO)
    }
    val offset = if(concept.offset.isEmpty) {
      null
    } else {
      SqlLiteral.createExactNumeric(concept.offset.get.toString, SqlParserPos.ZERO)
    }
    val fetch = if (concept.limit.isEmpty) {
      null
    } else {
      SqlLiteral.createExactNumeric(concept.limit.get.toString, SqlParserPos.ZERO)
    }
    new SqlSelect(
      SqlParserPos.ZERO,
      keywords,
      selectList,
      joins,
      where,
      groupBy,
      having,
      windowDecls,
      orderBy,
      offset,
      fetch,
      null
    )
  }

  def attributesToSqlNodeList(attributes: List[Attribute]): SqlNodeList = {
    new SqlNodeList(attributes.map(attributeToSqlNode).asJava, SqlParserPos.ZERO)
  }

  def attributeToSqlNode(attribute: Attribute): SqlNode = {
    if(attribute.value.isEmpty) {
      expressionConverter.sensExpressionToSqlNode(ConceptAttribute(Nil, attribute.name))
    } else {
      val operand = expressionConverter.sensExpressionToSqlNode(attribute.value.get)
      val identifier = new SqlIdentifier(List(attribute.name).asJava, SqlParserPos.ZERO)
      new SqlBasicCall(
        SqlStdOperatorTable.AS,
        List(operand, identifier).asJava,
        SqlParserPos.ZERO)
    }
  }

  def parentConceptsToJoin(parentConcepts: List[ParentConcept]): SqlNode = {
    var allJoins: SqlNode = prepareParentConcept(parentConcepts.head)
    var isPrevConceptOptional = parentConcepts.head.annotations.contains(Annotation.OPTIONAL)
    for (parentConcept <- parentConcepts.tail) {
      val curParentConceptNode = prepareParentConcept(parentConcept)
      val condition = prepareJoinCondition(parentConcept)
      val isCurConceptOptional = parentConcept.annotations.contains(Annotation.OPTIONAL)
      val joinType = if (!isPrevConceptOptional && !isCurConceptOptional) {
        if(condition == null) {
          JoinType.COMMA.symbol(SqlParserPos.ZERO)
        } else {
          JoinType.INNER.symbol(SqlParserPos.ZERO)
        }
      } else if (!isPrevConceptOptional && isCurConceptOptional) {
        JoinType.LEFT.symbol(SqlParserPos.ZERO)
      } else if (isPrevConceptOptional && !isCurConceptOptional) {
        JoinType.RIGHT.symbol(SqlParserPos.ZERO)
      } else {
        JoinType.FULL.symbol(SqlParserPos.ZERO)
      }
      allJoins = new SqlJoin(
        SqlParserPos.ZERO,
        allJoins.clone(SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        joinType,
        curParentConceptNode,
        if (condition == null) JoinConditionType.NONE.symbol(SqlParserPos.ZERO) else JoinConditionType.ON.symbol(SqlParserPos.ZERO),
        condition
      )

      //if at least one of the tables is not optional then the cumulative join is considered as non optional
      isPrevConceptOptional = isPrevConceptOptional && isCurConceptOptional
    }
    allJoins
  }

  def prepareParentConcept(parentConcept: ParentConcept): SqlNode = {
    val parentConceptNode = parentConcept.concept match {
      case ref: ConceptReference => {
        val conDefOpt = validationContext.getConcept(ref.getName)
        if (conDefOpt.isEmpty) {
          throw ElementNotFoundException(ref.getName)
        }
        conDefOpt.get.concept match {
          //TODO: implement function call
          case funDef: FunctionConcept => prepareFunctionScan(funDef, parentConcept.attributeDependencies)
          case dataSourceDef: DataSourceConcept => new SqlIdentifier(List(dataSourceDef.source.name).asJava, SqlParserPos.ZERO)
          case other => new SqlIdentifier(List(other.getMaterializationName).asJava, SqlParserPos.ZERO)
        }
      }
      case anonDef: AnonymousConceptDefinition => {
        prepareQuery(anonDef.toConcept())
      }
      case _ => throw new NotImplementedError()
    }
    val identifier = new SqlIdentifier(List(parentConcept.getAlias).asJava, SqlParserPos.ZERO)
    new SqlBasicCall(
      SqlStdOperatorTable.AS,
      List(parentConceptNode, identifier).asJava,
      SqlParserPos.ZERO)
  }

  def prepareJoinCondition(parentConcept: ParentConcept): SqlNode = {
    val inputAttrsList = extractInputAttributes(parentConcept)
    val joinConditions = parentConcept.attributeDependencies
      .filter(kv => !inputAttrsList.contains(kv._1))
      .map(curDep => {
        val leftField = new SqlIdentifier(List(parentConcept.getAlias, curDep._1).asJava, SqlParserPos.ZERO)
        val rightExpr = expressionConverter.sensExpressionToSqlNode(curDep._2)
        new SqlBasicCall(
          SqlStdOperatorTable.EQUALS,
          (leftField :: rightExpr :: Nil).asJava,
          SqlParserPos.ZERO)
      }).toList
    if (joinConditions.isEmpty) {
      null
    } else if(joinConditions.size == 1) {
      joinConditions.head
    } else {
      new SqlBasicCall(
        SqlStdOperatorTable.AND,
        joinConditions.asJava,
        SqlParserPos.ZERO)
    }
  }

  def extractInputAttributes(parentConcept: ParentConcept): List[String] = {
    parentConcept.concept match {
      case ref: ConceptReference => {
        val conDefOpt = validationContext.getConcept(ref.getName)
        if (conDefOpt.isEmpty) {
          throw ElementNotFoundException(ref.getName)
        }
        conDefOpt.get.concept match {
          case conDef: FunctionConcept => conDef.getInputAttributes.map(_.name)
          case _ => Nil
        }
      }
      case _ => Nil
    }
  }

  def prepareFunctionScan(concept: FunctionConcept, dependencies: Map[String, SensExpression]): SqlCall = {
    val operator = config.getOperatorTable.getOperatorList.asScala.find(op => op.getName == concept.name)
    if (operator.isEmpty) {
      throw ElementNotFoundException(concept.name)
    }
    val operands: List[SqlNode] = concept.getInputAttributes
      .map(attr => {
        val attrExpr = dependencies.get(attr.name)
        if (attrExpr.isEmpty) {
          throw AttributeExpressionNotFound(attr.name)
        }
        expressionConverter.sensExpressionToSqlNode(attrExpr.get)
      })
    new SqlBasicCall(
      operator.get,
      operands.asJava,
      SqlParserPos.ZERO)
  }

  def prepareDataSourceQuery(concept: DataSourceConcept): SqlCall = {
    val selectList: SqlNodeList = attributesToSqlNodeList(concept.attributes)
    val from: SqlNode = new SqlIdentifier(List(concept.source.name).asJava, SqlParserPos.ZERO)
    new SqlSelect(
      SqlParserPos.ZERO,
      null,
      selectList,
      from,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )
  }

  def prepareParentConceptQuery(parentConceptExpr: SensConceptExpression, aliasOpt: Option[String]): SqlNode = {
    val alias = aliasOpt.getOrElse("t")
    parentConceptExpr match {
      case ref: ConceptReference => {
        val conDefOpt = validationContext.getConcept(ref.getName)
        if (conDefOpt.isEmpty) {
          throw ElementNotFoundException(ref.getName)
        }
        val attributes: List[Attribute] = conDefOpt.get.concept.getAttributes(context)
        val anonConDef = AnonymousConceptDefinition.builder(
          attributes.map(attr => {
            Attribute(attr.name, Some(ConceptAttribute(alias :: Nil, attr.name)), Nil)
          }),
          ParentConcept(ConceptReference(ref.conceptName), Some(alias), Map(), Nil) :: Nil
        ).build()
        toSqlNode(anonConDef.toConcept())
      }
      case anonDef: AnonymousConceptDefinition =>
        val conDef = anonDef.toConcept()
        toSqlNode(conDef)
      case _ => throw new NotImplementedError()
    }
  }
}

object ConceptToSqlConverter {
  def create(context: ValidationContext): ConceptToSqlConverter = {
    val (config, _) = ConceptToRelConverter.prepareSchemaAndConfig(context)
    new ConceptToSqlConverter(context, config)
  }
}
