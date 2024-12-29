package org.sens.converter.rel

import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.sql.{SqlDelete, SqlIdentifier, SqlInsert, SqlMerge, SqlNodeList, SqlUpdate, SqlValuesOperator}
import org.apache.calcite.sql.ddl.{SqlCreateView, SqlDdlNodes, SqlDropView}
import org.apache.calcite.sql.fun.{SqlRowOperator, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.FrameworkConfig
import org.sens.core.concept.{Annotation, Concept, SensConcept}
import org.sens.core.expression.{ConceptAttribute, ListInitialization, SensExpression}
import org.sens.core.expression.literal.{ListLiteral, SensLiteral, StringLiteral}
import org.sens.parser.{GenericDefinitionException, ValidationContext, ValidationException, WrongArgumentsTypeException, WrongFunctionArgumentsException}
import org.sens.core.expression.operation.comparison._
import org.sens.core.expression.operation.logical.{And, AndSeq}
import org.sens.core.expression.operation.relational.InList

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.util.{Failure, Success, Try}

class ConceptSqlMaterializer(context: ValidationContext, config: FrameworkConfig) {
  val composer = new ConceptSqlComposer(context, config)
  val sqlConverter = new ExpressionsToSqlConverter(context, config, null)

  def materializeSql(concept: SensConcept, fullRefresh: Boolean = true, value: Option[SensExpression]= None): String = {
    val materialization = concept.getMaterialization

    if(concept.isGeneric && materialization != Annotation.MATERIALIZED_EPHEMERAL) {
      throw new ValidationException(s"Generic concept ${concept.getName} cannot be materialized")
    }

    val materializationNameStr = materialization.attributes.getOrElse(Annotation.TARGET_NAME, StringLiteral(concept.getName))
    val materializationName = materializationNameStr match {
      case StringLiteral(name) => name
      case _ => throw new ValidationException(s"Concept ${concept.getName}: Unsupported materialization target name type")
    }

    materialization.attributes.get(Annotation.TYPE) match {
      case None => ""
      case Some(StringLiteral(Annotation.EPHEMERAL_TYPE)) => ""
      case Some(StringLiteral(Annotation.TABLE_TYPE)) =>
        fullTableReload(materializationName, concept)
      case Some(StringLiteral(Annotation.VIEW_TYPE)) =>
        reloadView(materializationName, concept)
      case Some(StringLiteral(Annotation.MATERIALIZED_VIEW_TYPE)) =>
        reloadMaterializedView(materializationName, concept)
      case Some(StringLiteral(Annotation.INCREMENTAL_TYPE)) =>
        prepareIncrementalMaterialization(materializationName, concept, materialization.attributes, fullRefresh, value)
      case _ => throw new NotImplementedError()
    }
  }

  def fullTableReload(materializationName: String, concept: SensConcept): String = {
    val identifier = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val query = composer.composeToSqlNode(concept)
    val dropStmt = SqlDdlNodes.dropTable(SqlParserPos.ZERO, true, identifier)
    val createStmt = SqlDdlNodes.createTable(SqlParserPos.ZERO, false, false, identifier, null, query)
    composer.sqlConverter.expressionConverter.toSql(dropStmt) + ";\n" + composer.sqlConverter.expressionConverter.toSql(createStmt) + ";"
  }

  def reloadView(materializationName: String, concept: SensConcept): String = {
    val identifier = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val query = composer.composeToSqlNode(concept)
    val dropStmt = SqlDdlNodes.dropView(SqlParserPos.ZERO, true, identifier)
    val createStmt = SqlDdlNodes.createView(SqlParserPos.ZERO, false, identifier, null, query)
    composer.sqlConverter.expressionConverter.toSql(dropStmt) + ";\n" + composer.sqlConverter.expressionConverter.toSql(createStmt) + ";"
  }

  def reloadMaterializedView(materializationName: String, concept: SensConcept): String = {
    val identifier = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val query = composer.composeToSqlNode(concept)
    val dropStmt = SqlDdlNodes.dropMaterializedView(SqlParserPos.ZERO, true, identifier)
    val createStmt = SqlDdlNodes.createMaterializedView(SqlParserPos.ZERO, false, false, identifier, null, query)
    composer.sqlConverter.expressionConverter.toSql(dropStmt) + ";\n" + composer.sqlConverter.expressionConverter.toSql(createStmt) + ";"
  }

  def prepareIncrementalMaterialization(materializationName: String, concept: SensConcept, params: Map[String, SensExpression], fullRefresh: Boolean, value: Option[SensExpression]): String = {
    if(!fullRefresh) {
      val strategy = params.getOrElse(Annotation.STRATEGY_TYPE, StringLiteral(Annotation.DELETE_INSERT_STRATEGY))

      val curValue: SensExpression = value.getOrElse(
        params.getOrElse(Annotation.DEFAULT, throw new ValidationException(s"Concept ${concept.getName}: Value is not defined for incremental materialization"))
      )

      val uniqueKeyStr = params.getOrElse(
        Annotation.UNIQUE_KEY,
        throw new ValidationException(s"Concept ${concept.getName}: Incremental materialization requires non-empty unique key annotation")
      )
      val uniqueKey = uniqueKeyStr match {
        case attr: ConceptAttribute => attr.attribute
        case StringLiteral(attr) => attr
        case _ => throw new ValidationException(s"Concept ${concept.getName}: Unsupported unique key type for incremental materialization")
      }
      val operatorStr = params.getOrElse(Annotation.OPERATOR, StringLiteral("="))
      val operator = operatorStr match {
        case StringLiteral(op) => op
        case _ => throw new ValidationException(s"Concept ${concept.getName}: Unsupported operator type for incremental materialization")
      }
      val condition = prepareIncrementalCondition(operator, uniqueKey, curValue, Nil)
      val conceptWithCondition = addIncrementalCondition(concept, condition)

      strategy match {
        case StringLiteral(Annotation.DELETE_INSERT_STRATEGY) =>
          deleteInsertTable(materializationName, conceptWithCondition, condition)
        case StringLiteral(Annotation.INSERT_OVERRIDE_MERGE_STRATEGY) =>
          prepareInsertOverrideMergeMaterialization(materializationName, conceptWithCondition, uniqueKey, operator, curValue)
        case StringLiteral(Annotation.MERGE_STRATEGY) =>
          prepareMergeMaterialization(materializationName, conceptWithCondition, uniqueKey)
        case StringLiteral(Annotation.APPEND_STRATEGY) =>
          insertTable(materializationName, conceptWithCondition)
        case _ => throw new NotImplementedError()
      }
    } else {
      fullTableReload(materializationName, concept)
    }
  }

  def addIncrementalCondition(concept: SensConcept, expression: SensExpression): SensConcept = {
    concept match {
      case c: Concept => {
        val dependencies = c.attributeDependencies
        val updatedDependencies = dependencies match {
          case Some(And(op1, op2)) => AndSeq(expression :: op1 :: op2 :: Nil)
          case Some(AndSeq(ops)) => AndSeq(expression :: ops)
          case Some(exp) => AndSeq(expression :: exp :: Nil)
          case None => expression
        }
        val updatedConcept = c.copy(attributeDependencies = Some(updatedDependencies))
        updatedConcept.validateAndRemoveVariablePlaceholders(context) match {
          case Success(value) => value
          case Failure(exception) => throw exception
        }
      }
      case _ => throw new NotImplementedError()
    }
  }

  def prepareIncrementalCondition(operator: String, uniqueKey: String, value: SensExpression, conceptChain: List[String]): SensExpression = {
    operator match {
      case "=" => Equals(ConceptAttribute(conceptChain, uniqueKey), value)
      case ">" => GreaterThan(ConceptAttribute(conceptChain, uniqueKey), value)
      case ">=" => GreaterOrEqualsThan(ConceptAttribute(conceptChain, uniqueKey), value)
      case "<=" => LessOrEqualsThan(ConceptAttribute(conceptChain, uniqueKey), value)
      case "<" => LessThan(ConceptAttribute(conceptChain, uniqueKey), value)
      case "in" => {
        val operand = ConceptAttribute(conceptChain, uniqueKey)
        val list = value match {
          case l: ListInitialization => l
          case ListLiteral(items) => ListInitialization(items)
          case other => ListInitialization(other :: Nil)
        }
        InList(operand, list)
      }
      case "between" => {
        val operand = ConceptAttribute(conceptChain, uniqueKey)
        val limits = value match {
          case ListInitialization(items) => items
          case ListLiteral(items) => items
          case other => throw WrongArgumentsTypeException("Between", other.getClass.getSimpleName, ListInitialization.getClass.getSimpleName)
        }
        if (limits.size > 1) {
          Between(operand, limits(0), limits(1))
        } else {
          throw WrongFunctionArgumentsException("between", limits.size)
        }
      }
      case _ => throw new NotImplementedError()
    }
  }

  def deleteInsertTable(materializationName: String, concept: SensConcept, condition: SensExpression): String = {
    val tableId = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val conditionNode = composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(condition)
    val deleteStmt = new SqlDelete(SqlParserPos.ZERO, tableId, conditionNode, null, null)
    val source = composer.composeToSqlNode(concept)
    val insertStmt = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, tableId, source, null)
    composer.sqlConverter.expressionConverter.toSql(deleteStmt) + ";\n" + composer.sqlConverter.expressionConverter.toSql(insertStmt) + ";"
  }

  def insertTable(materializationName: String, concept: SensConcept): String = {
    val tableId = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val source = composer.composeToSqlNode(concept)
    val insertStmt = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, tableId, source, null)
    composer.sqlConverter.expressionConverter.toSql(insertStmt) + ";"
  }

  def prepareInsertOverrideMergeMaterialization(materializationName: String, concept: SensConcept, uniqueKey: String, operator: String, value: SensExpression): String = {
    val targetTable = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val targetAlias = new SqlIdentifier(List("sens_merge_target").asJava, SqlParserPos.ZERO)
    val targetTableSql = composer.sqlConverter.expressionConverter.toSql(
      SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, targetTable, targetAlias)
    )

    val attributeNames = concept.getAttributes(context).map(_.name)

    val source = composer.composeToSqlNode(concept)
    val sourceAlias = new SqlIdentifier(List("sens_merge_source").asJava, SqlParserPos.ZERO)
    val sourceSql = composer.sqlConverter.expressionConverter.toSql(
      SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, source, sourceAlias)
    )

    val condition = composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(
      Equals(ConceptAttribute("sens_merge_source" :: Nil, uniqueKey), ConceptAttribute("sens_merge_target" :: Nil, uniqueKey))
    )
    val cond = prepareIncrementalCondition(operator, uniqueKey, value, "sens_merge_target" :: Nil)
    val condSql = composer.sqlConverter.expressionConverter.sensExpressionToSql(cond)

    val targetColumnsIdentifiers = attributeNames.map(attr =>
      new SqlIdentifier(List(attr).asJava, SqlParserPos.ZERO)
    )
    val targetColumns = new SqlRowOperator("").createCall(SqlParserPos.ZERO, targetColumnsIdentifiers.asJava)
    val targetColumnsSql = composer.sqlConverter.expressionConverter.toSql(targetColumns)
    val insertExpressionList = attributeNames
      .map(ConceptAttribute("sens_merge_source" :: Nil, _))
      .map(item => composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(item))
    val row = new SqlRowOperator("").createCall(SqlParserPos.ZERO, insertExpressionList.asJava)
    val valuesList = new SqlValuesOperator().createCall(
      SqlParserPos.ZERO,
      row
    )
    val insertValuesSql = composer.sqlConverter.expressionConverter.toSql(valuesList)
    val attributeUpdates = attributeNames
      .filter(_ != uniqueKey)
      .map(attr => Equals(ConceptAttribute("sens_merge_target" :: Nil, attr), ConceptAttribute("sens_merge_source" :: Nil, attr)))
      .map(composer.sqlConverter.expressionConverter.sensExpressionToSqlNode)
    val updateRow = new SqlRowOperator("").createCall(SqlParserPos.ZERO, attributeUpdates.asJava)
    val updateSql = composer.sqlConverter.expressionConverter.toSql(updateRow)



    s"""MERGE $targetTableSql
       |USING $sourceSql
       |ON $condition
       |AND $condSql
       |WHEN NOT MATCHED BY TARGET THEN
       |INSERT $targetColumnsSql
       |$insertValuesSql
       |WHEN MATCHED THEN UPDATE SET
       |$updateSql
       |WHEN NOT MATCHED BY SOURCE
       |AND $condSql
       |THEN DELETE;""".stripMargin
  }

  def prepareMergeMaterialization(materializationName: String, concept: SensConcept, uniqueKey: String): String = {
    val targetTable = new SqlIdentifier(List(materializationName).asJava, SqlParserPos.ZERO)
    val targetAlias = new SqlIdentifier(List("sens_merge_target").asJava, SqlParserPos.ZERO)
    val sourceSql = composer.composeToSqlNode(concept)
    val sourceAlias = new SqlIdentifier(List("sens_merge_source").asJava, SqlParserPos.ZERO)
    val source = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, sourceSql, sourceAlias)
    val condition = composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(
      Equals(ConceptAttribute("sens_merge_source" :: Nil, uniqueKey), ConceptAttribute("sens_merge_target" :: Nil, uniqueKey))
    )

    val attributeNames = concept
      .getAttributes(context)
      .map(_.name)

    val targetColumnList = attributeNames
      .filter(_ != uniqueKey)
      .map(ConceptAttribute("sens_merge_target" :: Nil, _))
      .map(item => composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(item))
    val sourceExpressionList = attributeNames
      .filter(_ != uniqueKey)
      .map(ConceptAttribute("sens_merge_source" :: Nil, _))
      .map(item => composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(item))
    val update = new SqlUpdate(
      SqlParserPos.ZERO,
      targetTable,
      new SqlNodeList(targetColumnList.asJava, SqlParserPos.ZERO),
      new SqlNodeList(sourceExpressionList.asJava, SqlParserPos.ZERO),
      null,
      null,
      null)

    val insertExpressionList = attributeNames
      .map(ConceptAttribute("sens_merge_source" :: Nil, _))
      .map(item => composer.sqlConverter.expressionConverter.sensExpressionToSqlNode(item))
    val row = new SqlRowOperator("").createCall(SqlParserPos.ZERO, insertExpressionList.asJava)
    val valuesList = new SqlValuesOperator().createCall(
      SqlParserPos.ZERO,
      row
    )
    val targetColumnsIdentifiers = attributeNames.map(attr =>
      new SqlIdentifier(List(attr).asJava, SqlParserPos.ZERO)
    )
    val insert = new SqlInsert(
      SqlParserPos.ZERO,
      SqlNodeList.EMPTY,
      targetTable,
      valuesList,
      new SqlNodeList(targetColumnsIdentifiers.asJava, SqlParserPos.ZERO))

    val mergeStmt = new SqlMerge(
      SqlParserPos.ZERO,
      targetTable,
      condition,
      source,
      update,
      insert,
      null,
      targetAlias
    )
    composer.sqlConverter.expressionConverter.toSql(mergeStmt) + ";"
  }

}

object ConceptSqlMaterializer {
  def create(context: ValidationContext): ConceptSqlMaterializer = {
    val (config, _) = ConceptToRelConverter.prepareSchemaAndConfig(context)
    new ConceptSqlMaterializer(context, config)
  }
}
