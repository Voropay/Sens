package org.sens.converter.rel

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeFieldImpl, RelDataTypeSystem}
import org.apache.calcite.rel.core.{JoinRelType, TableModify}
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.rex.RexNode
import org.apache.calcite.schema.impl.ListTransientTable
import org.apache.calcite.sql.{SqlIdentifier, SqlKind}
import org.apache.calcite.sql.`type`.{BasicSqlType, InferTypes, ReturnTypes, SqlReturnTypeInference, SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.{SqlParser, SqlParserPos}
import org.apache.calcite.sql.util.SqlOperatorTables
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}
import org.sens.core.concept.{AggregationConcept, Annotation, Attribute, Concept, CubeConcept, CubeInheritedConcept, DataSourceConcept, FunctionConcept, InheritedConcept, IntersectConcept, MinusConcept, Order, ParentConcept, SensConcept, UnionConcept}
import org.sens.core.expression.{ConceptAttribute, SensExpression}
import org.sens.core.expression.literal.BooleanLiteral
import org.sens.core.expression.function.FunctionReference
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.parser.{AttributeExpressionNotFound, ElementNotFoundException, ValidationContext}

import java.util.{Collections, Properties}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class ConceptToRelConverter(context: ValidationContext, config: FrameworkConfig, catalogReader: CalciteCatalogReader) {
  val validationContext = context.addFrame
  val expressionsConverter = new ExpressionsToRelConverter(config, validationContext, this)

  /*
1. prepare schema - get attributes of parent concepts and register tables for them
2. prepare scans and joins
3. prepare filters
4. prepare project
5. prepare aggregate
6. prepare having clause
7. prepare order and limit
 */

  def toRel(concept: SensConcept): RelNode = {

    val relBuilder = RelBuilder.create(config)

    toRel(concept, relBuilder)
  }

  def toRel(concept: SensConcept, relBuilder: RelBuilder, numberOfTables: Int = 1): RelNode = {
    validationContext.setCurrentConcept(concept)
    concept match {
      case conceptDef: Concept => {
        prepareQuery(conceptDef, relBuilder, numberOfTables)
      }
      case cubeDef: CubeConcept => {
        val conceptDef = cubeDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef, relBuilder, numberOfTables)
      }
      case cubeInheritedDef: CubeInheritedConcept => {
        val conceptDef = cubeInheritedDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef, relBuilder, numberOfTables)
      }
      case inheritedDef: InheritedConcept => {
        val conceptDef = inheritedDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef, relBuilder, numberOfTables)
      }
      case aggregationDef: AggregationConcept => {
        val conceptDef = aggregationDef.inferAttributeExpressions(validationContext).get
        prepareQuery(conceptDef, relBuilder, numberOfTables)
      }
      case intersectConceptDef: IntersectConcept => {
        val parentConceptsQueries = intersectConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConcept(pc, Nil, relBuilder, numberOfTables).build)
        relBuilder.pushAll(parentConceptsQueries.asJava)
        relBuilder.intersect(true).build()
      }
      case unionConceptDef: UnionConcept => {
        val parentConceptsQueries = unionConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConcept(pc, Nil, relBuilder, numberOfTables).build)
        relBuilder.pushAll(parentConceptsQueries.asJava)
        relBuilder.union(true).build()
      }
      case minusConceptDef: MinusConcept => {
        val parentConceptsQueries = minusConceptDef
          .inferAttributeExpressions(context)
          .get
          .parentConcepts.map(pc => prepareParentConcept(pc, Nil, relBuilder, numberOfTables).build)
        relBuilder.pushAll(parentConceptsQueries.asJava)
        relBuilder.minus(true).build()
      }
      case _ => throw new NotImplementedError()
    }
  }

  def toDeleteRel(materializationName: String, condition: SensExpression): RelNode = {
    val relBuilder = RelBuilder.create(config)
    val tableScan = relBuilder.scan(materializationName)
    val filters = prepareFilters(Some(condition), Nil, tableScan, 1)
    val node = filters.build()

    val table = catalogReader.getTableForMember(List(materializationName).asJava)
    LogicalTableModify.create(
      table,
      catalogReader,
      node,
      TableModify.Operation.DELETE,
      null,
      null,
      false);
  }

  def prepareQuery(concept: Concept, relBuilder: RelBuilder, numberOfTables: Int): RelNode = {
    val joins = prepareJoins(concept.parentConcepts, concept.attributes, relBuilder, numberOfTables)
    val filters = prepareFilters(concept.attributeDependencies, concept.attributes, joins, numberOfTables)
    val projections = if(isAggregateConcept(concept)) {
      val aggregates = prepareAggregate(concept.attributes, concept.groupByAttributes,  filters, numberOfTables)
      prepareFilters(concept.groupDependencies, Nil, aggregates, numberOfTables)
    } else {
      prepareProjections(concept.attributes, concept.attributes, concept.annotations, filters, numberOfTables)
    }
    val limits = prepareSortAndLimit(concept.orderByAttributes, concept.limit, concept.offset, projections, numberOfTables)
    limits.build()
  }

  def isAggregateConcept(concept: Concept): Boolean = {
    concept.groupByAttributes.nonEmpty || concept.attributes.exists(attr =>
      attr.findSubExpression {
        case FunctionReference(name) if StandardSensFunctions.standardSensAggregateFunctions.contains(name) => true
        case _ => false
      }.isDefined
    )

  }

  def prepareJoins(parentConcepts: List[ParentConcept], childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int = 1): RelBuilder = {

    var relBuilderWithScans = prepareParentConcept(parentConcepts.head, childAttributes, relBuilder, numberOfTables).as(parentConcepts.head.getAlias)
    var isPrevConceptOptional = parentConcepts.head.annotations.contains(Annotation.OPTIONAL)
    for(parentConcept <- parentConcepts.tail) {
      val isCurConceptOptional = parentConcept.annotations.contains(Annotation.OPTIONAL)
      val joinType =  if(!isPrevConceptOptional && !isCurConceptOptional) {
        JoinRelType.INNER
      } else if(!isPrevConceptOptional && isCurConceptOptional) {
        JoinRelType.LEFT
      } else if(isPrevConceptOptional && !isCurConceptOptional) {
        JoinRelType.RIGHT
      } else {
        JoinRelType.FULL
      }
      relBuilderWithScans = prepareParentConcept(parentConcept, childAttributes, relBuilderWithScans, numberOfTables)
        .as(parentConcept.getAlias)
        .join(
          joinType,
          prepareJoinConditions(parentConcept, childAttributes, relBuilderWithScans, numberOfTables + 1).asJava)
      //if at least one of the tables is not optional then the cumulative join is considered as non optional
      isPrevConceptOptional = isPrevConceptOptional && isCurConceptOptional
    }

    relBuilderWithScans
  }

  def prepareParentConcept(parentConcept: ParentConcept, childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    val parentConceptExpr = parentConcept.concept
    parentConceptExpr match {
      case ref: ConceptReference => {
        val conDefOpt = validationContext.getConcept(ref.getName)
        if(conDefOpt.isEmpty) {
          throw ElementNotFoundException(ref.getName)
        }
        conDefOpt.get.concept match {
          case conDef: FunctionConcept => prepareFunctionScan(conDef, parentConcept.attributeDependencies, childAttributes, relBuilder, numberOfTables)
          case other => {
            val materializationName = other.getMaterializationName
            relBuilder.scan(materializationName)
          }
        }
      }
      case anonDef: AnonymousConceptDefinition => {
        val nestedConceptDef = anonDef.toConcept
        val subQuery = prepareQuery(nestedConceptDef, relBuilder, numberOfTables)
        relBuilder.push(subQuery)
      }
      case _ => throw new NotImplementedError()
    }
  }

  def prepareFunctionScan(concept: FunctionConcept, dependencies: Map[String, SensExpression], childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    val operator = config.getOperatorTable.getOperatorList.asScala.find(op => op.getName == concept.name)
    if(operator.isEmpty) {
      throw ElementNotFoundException(concept.name)
    }
    val operands: List[RexNode] = concept.getInputAttributes
      .map(attr => {
        val attrExpr = dependencies.get(attr.name)
        if(attrExpr.isEmpty) {
          throw AttributeExpressionNotFound(attr.name)
        }
        expressionsConverter.sensExpressionToRexNode(attrExpr.get, childAttributes, relBuilder, numberOfTables)
      })
    relBuilder.functionScan(operator.get, 0, operands.asJava)
  }

  def prepareJoinConditions(parentConcept: ParentConcept, childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int): List[RexNode] = {
    val inputAttrsList = extractInputAttributes(parentConcept)
    val joinConditions = parentConcept.attributeDependencies
      .filter(kv => !inputAttrsList.contains(kv._1))
      .map(curDep => {
      val leftField = relBuilder.field(numberOfTables, parentConcept.getAlias, curDep._1)
      val rightExpr = expressionsConverter.sensExpressionToRexNode(curDep._2, childAttributes, relBuilder, numberOfTables)
      val res = relBuilder.call(
        SqlStdOperatorTable.EQUALS,
        leftField,
        rightExpr)
      res
    }).toList
    if(joinConditions.isEmpty) {
      expressionsConverter.sensExpressionToRexNode(BooleanLiteral(true), childAttributes, relBuilder, numberOfTables) :: Nil
    } else {
      joinConditions
    }
  }

  def extractInputAttributes(parentConcept: ParentConcept): List[String] = {
    parentConcept.concept match {
      case ref: ConceptReference => {
        val conDefOpt = validationContext.getConcept(ref.getName)
        if(conDefOpt.isEmpty) {
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

  def prepareFilters(dependencies: Option[SensExpression], childAttributes: List[Attribute], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    if(dependencies.isDefined) {
      val conditions = expressionsConverter.sensExpressionToRexNode(dependencies.get, childAttributes, relBuilder, numberOfTables)
      relBuilder.filter(conditions)
    } else {
      relBuilder
    }
  }

  def prepareProjections(attributes: List[Attribute], childAttributes: List[Attribute], annotations: List[Annotation], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    val projections = attributes.map(curAttr => {
      relBuilder.alias(expressionsConverter.sensExpressionToRexNode(curAttr.value.get, childAttributes, relBuilder, numberOfTables), curAttr.name)
    })
    val projection = relBuilder.project(projections.asJava, ImmutableList.of, true)
    if(annotations.contains(Annotation.UNIQUE)) {
      projection.distinct()
    } else {
      projection
    }

  }

  def prepareAggregate(attributes: List[Attribute], groupByAttributes: List[SensExpression], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    val groupByExpressions: List[RexNode] = groupByAttributes.map(curExpr =>
      expressionsConverter.sensExpressionToRexNode(curExpr, attributes, relBuilder, numberOfTables)
    )
    val aggregateAttributes = attributes.filter(curAttr => !groupByAttributes.contains(ConceptAttribute(Nil, curAttr.name)))
    val aggregateExpressions = aggregateAttributes.map(expressionsConverter.attributeToAggregateCall(_, relBuilder, numberOfTables))
    relBuilder.aggregate(
      relBuilder.groupKey(groupByExpressions.asJava),
      aggregateExpressions.asJava
    )
  }


  def prepareSortAndLimit(sortOrder: List[Order], limit: Option[Int], offset : Option[Int], relBuilder: RelBuilder, numberOfTables: Int): RelBuilder = {
    if(limit.isEmpty && offset.isEmpty && sortOrder.isEmpty) {
      relBuilder
    } else {
      val limitValue = limit.getOrElse(-1)
      val offsetValue = offset.getOrElse(-1)
      if(sortOrder.isEmpty) {
        relBuilder.limit(offsetValue, limitValue)
      } else {
        val sortNodes = sortOrder.map(curOrder => {
          val node = expressionsConverter.sensExpressionToRexNode(
            curOrder.attribute,
            Nil, relBuilder,
            numberOfTables)
          if(curOrder.direction == Order.ASC) {
            node
          } else {
            relBuilder.desc(node)
          }
        })
        relBuilder.sortLimit(offsetValue, limitValue, sortNodes.asJava)
      }
    }
  }
}

object ConceptToRelConverter {

  def create(context: ValidationContext): ConceptToRelConverter = {
    val (config, catalogReader) = prepareSchemaAndConfig(context)
    new ConceptToRelConverter(context, config, catalogReader)
  }

  def prepareSchemaAndConfig(context: ValidationContext): (FrameworkConfig, CalciteCatalogReader) = {
    val schema = new VirtualSchema
    val typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)

    val concepts = context.getConcepts

    var tableFunctionSqlOperators: List[SqlUserDefinedFunction] = List()
    concepts.foreach(conDef => {
      var parentConceptType = new RelDataTypeFactory.Builder(typeFactory)
      if (!conDef.concept.isGeneric) {
        val attributes = conDef.concept.getAttributes(context)
        var i = 0;
        for (curAttr <- attributes) {
          val nullable = curAttr.annotations.contains(Annotation.OPTIONAL)
          val field = new RelDataTypeFieldImpl(
            curAttr.name,
            i,
            typeFactory.createTypeWithNullability(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ANY), nullable)
          )
          parentConceptType = parentConceptType.add(field)
          i += 1;
        }
        val relDataType = parentConceptType.build()

        conDef.concept match {
          case funcCon: FunctionConcept =>
            val inputArguments = funcCon.getInputAttributes.map(_.name)
            val returnType = ReturnTypes.explicit(relDataType)
            tableFunctionSqlOperators = prepareFunction(funcCon.name, inputArguments, returnType) :: tableFunctionSqlOperators
          /*
        case dsCon: DataSourceConcept =>
          val materializationName = dsCon.source.name
          val table = new ListTransientTable(materializationName, relDataType)
          schema.add(materializationName, table)
           */
          case _ =>
            val materializationName = conDef.concept.getMaterializationName
            val table = new ListTransientTable(materializationName, relDataType)
            schema.add(materializationName, table)
        }
      }
    })

    val schemaPlus = schema.plus
    tableFunctionSqlOperators.foreach(op => schemaPlus.add(op.getName, op.getFunction))

    val functions = context.getUserDefinedFunctions
    val userDefinedFunctionSqlOperators = functions.map(func => prepareFunction(func.name, func.arguments, ReturnTypes.VARCHAR_2000))
    userDefinedFunctionSqlOperators.foreach(op => schemaPlus.add(op.getName, op.getFunction))

    val sensFunctions = tableFunctionSqlOperators ++ userDefinedFunctionSqlOperators
    val funcOperatorTable = SqlOperatorTables.of(sensFunctions.asJava)
    val chainedOpTable = SqlOperatorTables.chain(ImmutableList.of(funcOperatorTable, SqlStdOperatorTable.instance()))

    val connectionConfig = new CalciteConnectionConfigImpl( new Properties())
    val catalogReader = new CalciteCatalogReader(
      schema,
      Collections.singletonList(schema.getName),
      typeFactory,
      connectionConfig
    )

    val config = Frameworks.newConfigBuilder()
      .parserConfig(SqlParser.Config.DEFAULT)
      .defaultSchema(schemaPlus)
      .operatorTable(chainedOpTable)
      .typeSystem(typeFactory.getTypeSystem)
      .build()
    (config, catalogReader)
  }

  def prepareFunction(name: String, arguments: List[String], returnType: SqlReturnTypeInference): SqlUserDefinedFunction = {
    val funcImpl = new PushedDownFunctionImpl(arguments)

    val funcIdentifier = new SqlIdentifier(
      Collections.singletonList(name),
      null,
      SqlParserPos.ZERO,
      null
    )

    val parametersList: List[RelDataType] = List.fill(arguments.size)(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ANY))
    new SqlUserDefinedFunction(
      funcIdentifier,
      SqlKind.OTHER_FUNCTION,
      returnType,
      InferTypes.explicit(parametersList.asJava),
      null,
      funcImpl)
  }
}
