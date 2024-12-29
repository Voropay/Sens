package org.sens.converter.rel

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.{SqlIdentifier, SqlNode, SqlNodeList, SqlWith}
import org.apache.calcite.sql.dialect.AnsiSqlDialect
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.FrameworkConfig
import org.sens.core.concept.{Annotation, CubeInheritedConcept, DataSourceConcept, FunctionConcept, SensConcept}
import org.sens.core.expression.concept.ConceptReference
import org.sens.parser.{ElementNotFoundException, ValidationContext}
import org.sens.core.expression.literal.StringLiteral

import scala.collection.JavaConverters.seqAsJavaListConverter


class ConceptSqlComposer(context: ValidationContext, config: FrameworkConfig) {
  val sqlConverter = new ConceptToSqlConverter(context, config)

  def composeSelect(concept: SensConcept): String = {
    val sqlNode = composeToSqlNode(concept)
    sqlConverter.expressionConverter.toSql(sqlNode)
  }

  def composeToSqlNode(concept: SensConcept): SqlNode = {
    val normalizedConcept = concept.inferAttributeExpressions(context).get
    val conceptSqlNode = sqlConverter.toSqlNode(normalizedConcept)
    val parentConceptsNodes: Map[String, SensConcept] = composeParentConcepts(normalizedConcept, Map())
    if(parentConceptsNodes.isEmpty) {
      return conceptSqlNode
    }
    val orderedCTEs = orderCTEs(parentConceptsNodes)
    val orderedCteSqlNodes = orderedCTEs.map(el => (el._1, sqlConverter.toSqlNode(el._2)))
    val withNodes = orderedCteSqlNodes.map(node => {
      val identifier = new SqlIdentifier(List(node._1).asJava, SqlParserPos.ZERO)
      SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, identifier, node._2)
    })
    val composedSqlNode = new SqlWith(
      SqlParserPos.ZERO,
      new SqlNodeList(withNodes.asJava, SqlParserPos.ZERO),
      conceptSqlNode
    )
    composedSqlNode
  }

  def composeParentConcepts(concept: SensConcept, cteList: Map[String, SensConcept]): Map[String, SensConcept] = {
    var parentConceptsCTEs = cteList

    val parentConceptRefs: List[ConceptReference] = concept.findAllSubExpressions {
      case _: ConceptReference => true
      case _ => false
    }.collect {
      case el: ConceptReference => el
    }

    parentConceptRefs.foreach(curRef => {
      val conceptName = curRef.getName
      if (!parentConceptsCTEs.contains(conceptName)) {
        val conceptOpt = context.getConcept(conceptName)
        if (conceptOpt.isEmpty) {
          throw ElementNotFoundException(conceptName)
        }
        val parentConceptDef = conceptOpt.get.concept
        if (isParentConceptComposable(parentConceptDef)) {
          if(!concept.isInstanceOf[CubeInheritedConcept]) {
            parentConceptsCTEs += (conceptName -> parentConceptDef)
          }
          parentConceptsCTEs = composeParentConcepts(parentConceptDef, parentConceptsCTEs)
        }
      }
    })

    parentConceptsCTEs
  }

  def isParentConceptComposable(parentConceptDef: SensConcept): Boolean = {
    val materialization = parentConceptDef
      .getAnnotations
      .find(_.name == Annotation.MATERIALIZED)
      .flatMap(
        _.attributes.get(Annotation.TYPE)
      )

    val ephemeral = materialization.isEmpty || materialization.get == StringLiteral(Annotation.EPHEMERAL_TYPE)
    parentConceptDef match {
      case _: DataSourceConcept => false
      case _: FunctionConcept => throw new NotImplementedError()
      case _ => ephemeral
    }

  }

  def orderCTEs(cteList: Map[String, SensConcept]): List[(String, SensConcept)] = {
    val toPred: Map[String, Set[String]] = cteList.map(element => {
      val edge = element._1
      val links = element._2.getParentConcepts(context).map(_.concept.getName).filter(cteList.contains).toSet
      (edge, links)
    })
    val orderedCteNames = Utils.tsort(toPred, Seq())
    orderedCteNames.map(name => (name, cteList(name))).toList
  }

  def toSql(relNode: RelNode): String = {
    val sqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT)
    val sqlNode = sqlConverter.visitRoot(relNode).asStatement()
    sqlNode.toSqlString(AnsiSqlDialect.DEFAULT).getSql
  }
}

object ConceptSqlComposer {
  def create(context: ValidationContext): ConceptSqlComposer = {
    val (config, _) = ConceptToRelConverter.prepareSchemaAndConfig(context)
    new ConceptSqlComposer(context, config)
  }
}
