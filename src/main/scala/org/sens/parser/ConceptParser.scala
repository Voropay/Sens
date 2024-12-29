package org.sens.parser

import org.sens.core.concept._
import org.sens.core.datasource.DataSource
import org.sens.core.expression.{ConceptAttribute, NamedElementPlaceholder, SensExpression, Variable, WindowFunction, WindowFunctions}
import org.sens.core.expression.concept._
import org.sens.core.datasource.{FileDataSource, FileFormats}
import org.sens.core.statement.StatementBlock

trait ConceptParser extends StatementsParser {

  override def sensConcept: Parser[SensConcept] = functionConceptDefinition | conceptDefinition | cubeConceptDefinition | cubeInheritedConceptDefinition | inheritedConceptDefinition |
    relationshipConceptDefinition | dataSourceConceptDefinition | unionConceptDefinition | intersectConceptDefinition | minusConceptDefinition

  def annotationParser: Parser[Annotation] = "@" ~ sensIdent ~ opt("(" ~ repsep(annotationKeyValuePair, ",") ~ ")") ^^ {
    case "@" ~ name ~ values  => {
      val valuesMap: Map[String, SensExpression] = if(values.isEmpty) Map() else values.get._1._2.toMap
      Annotation(name, valuesMap)
    }
  }
  def annotationKeyValuePair = sensIdent ~ "=" ~ sensExpression ^^ {value => Tuple2(value._1._1, value._2)}

  def conceptAttribute: Parser[ConceptAttribute] = (attributeExpression | namedElementExpression) ^^ {
    case value: NamedElementPlaceholder => ConceptAttribute(Nil, value.name)
    case value: ConceptAttribute => value
  }

  def orderParser: Parser[Order] = conceptAttribute ~ (orderAscKeyword | orderDescKeyword) ^^ {
    value => {
      val direction = if (value._2 == "asc") Order.ASC else Order.DESC
      Order(value._1, direction)
    }
  }

  def attributeParser: Parser[Attribute] = repsep(annotationParser, ",") ~ sensIdent ~ opt("=" ~> sensExpression) ^^ {value =>
    Attribute(value._1._2, value._2, value._1._1)
  }

  def parentConceptParser: Parser[ParentConcept] =
    repsep(annotationParser, ",") ~ conceptExpression ~ opt(sensIdent) ~ opt("(" ~> repsep(sensIdent ~ "=" ~ sensExpression, ",") <~ ")")  ^^ {
    case
      annotations ~ conceptExpr ~ alias ~ attrDependencies => {
      val attrDepMap: Map[String, SensExpression] = if (attrDependencies.isDefined)
        attrDependencies.get.map(item => (item._1._1, item._2)).toMap
      else
        Map()
      ParentConcept(
        conceptExpr,
        alias,
        attrDepMap,
        annotations)
    }
  }

  def conceptExpression: Parser[SensConceptExpression] = anonymousConceptExpression | anonymousFunctionConceptExpression | conceptReference

  override def anonymousConceptDefinition: Parser[SensConceptExpression] = anonymousConceptExpression | anonymousFunctionConceptExpression

  def conceptReference: Parser[SensConceptExpression] = sensConceptIdent ~ opt(paramsExpression) ^^ {
    case conceptIdent ~ paramsMap =>
      conceptIdent match {
        case ConstantIdent(name) if paramsMap.isEmpty => ConceptReference(name)
        case _ => GenericConceptReference(conceptIdent, paramsMap.getOrElse(Map()))
      }
  }

  def paramsExpression: Parser[Map[String, SensExpression]] = "[" ~> repsep(nameExpressionPair, ",") <~ "]" ^^ {
    value => value.toMap
  }

  def nameExpressionPair: Parser[(String, SensExpression)] = sensIdent ~ ":" ~ sensExpression ^^ {
    case name ~ ":" ~ value => (name, value)}

  def sensConceptIdent: Parser[SensIdent] = expressionIdent | constantIdent

  def constantIdent: Parser[ConstantIdent] = sensIdent ^^ {value => ConstantIdent(value)}

  def expressionIdent: Parser[ExpressionIdent] = "$" ~ sensExpression ^^ {value => ExpressionIdent(value._2)}

  def anonymousConceptExpression: Parser[AnonymousConceptDefinition] =
    "<" ~
    repsep(annotationParser, ",") ~
    opt("(" ~> rep1sep(attributeParser, ",") <~ ")") ~
    opt(fromKeyword) ~ rep1sep(parentConceptParser, ",") ~
    opt(whereKeyword ~> sensExpression) ~
    opt(groupByKeyword ~> rep1sep(sensExpression, ",") ~ opt(havingKeyword ~> sensExpression)) ~
    opt(orderByKeyword ~> rep1sep(orderParser, ",")) ~
    opt(limitKeyword ~> sensIntLiteral) ~
    opt(offsetKeyword ~> sensIntLiteral) ~
    ">" ^^ {
      case "<" ~ annotations ~ attributes ~ optFromKeyword ~ parentConceptsList ~ whereCond ~ groupByCond ~ orderByCond ~ limitCond ~ offsetCond ~ ">" =>
        val groupByAttributes = if(groupByCond.isDefined) {
          groupByCond.get._1
        } else {
          Nil
        }
        val groupByDependencies = if(groupByCond.isDefined) {
          groupByCond.get._2
        } else {
          None
        }
        AnonymousConceptDefinition(
          attributes.getOrElse(Nil),
          parentConceptsList,
          whereCond,
          groupByAttributes,
          groupByDependencies,
          orderByCond.getOrElse(Nil),
          limitCond.map(_.value),
          offsetCond.map(_.value),
          annotations
        )
    }



  def anonymousFunctionConceptExpression: Parser[AnonymousFunctionConceptDefinition] =
    "<" ~> repsep(sensStatement, rep1(statementsSeparator)) <~ rep(statementsSeparator) <~ ">" ^^ { value =>
      if(value.size > 1)
        AnonymousFunctionConceptDefinition(StatementBlock(value))
      else
        AnonymousFunctionConceptDefinition(value.head)
  }

  def conceptDefinition: Parser[Concept] =
    repsep(annotationParser, ",") ~
    conceptKeyword ~
    sensIdent ~
    opt("[" ~ rep1sep(sensIdent, ",") ~ "]") ~
    "(" ~ rep1sep(attributeParser, ",") ~ ")" ~
    fromKeyword ~ rep1sep(parentConceptParser, ",") ~
    opt(whereKeyword ~> sensExpression) ~
    opt(groupByKeyword ~> rep1sep(sensExpression, ",") ~ opt(havingKeyword ~> sensExpression)) ~
    opt(orderByKeyword ~> rep1sep(orderParser, ",")) ~
    opt(limitKeyword ~> sensIntLiteral) ~
    opt(offsetKeyword ~> sensIntLiteral) ^^ {
      case
        annotations ~
        "concept" ~ name ~
        genericParams ~
        "(" ~ attributes ~ ")" ~
        "from" ~ parentConcepts ~
        whereExpr ~
        groupByExpr ~
        orderByExpr ~
        limitExpr ~ offsetExpr => {
        val groupByAttributes = if (groupByExpr.isDefined) {
          groupByExpr.get._1
        } else {
          Nil
        }
        val groupByDependencies = if (groupByExpr.isDefined) {
          groupByExpr.get._2
        } else {
          None
        }
        Concept(
          name,
          genericParams.map(_._1._2).getOrElse(Nil),
          attributes,
          parentConcepts,
          whereExpr,
          groupByAttributes,
          groupByDependencies,
          orderByExpr.getOrElse(Nil),
          limitExpr.map(_.value),
          offsetExpr.map(_.value),
          annotations
        )
      }
    }

  def inheritedConceptDefinition: Parser[InheritedConcept] =
    repsep(annotationParser, ",") ~
    conceptKeyword ~
    sensIdent ~
    opt("[" ~ rep1sep(sensIdent, ",") ~ "]") ~
    isKeyword ~ rep1sep(parentConceptParser, ",") ~
    opt(withKeyword ~> rep1sep(attributeParser, ",")) ~
    opt(withoutKeyword ~> rep1sep(conceptAttribute, ",")) ~
    opt(whereKeyword ~> sensExpression) ~
    opt(orderByKeyword ~> rep1sep(orderParser, ",")) ~
    opt(limitKeyword ~> sensIntLiteral) ~
    opt(offsetKeyword ~> sensIntLiteral) ^^ {
    case
      annotations ~
      "concept" ~ name ~
      genericParams ~
      "is" ~ parentConcepts ~
      withExpr ~
      withoutExpr ~
      whereExpr ~
      orderByExpr ~
      limitExpr ~ offsetExpr =>
        InheritedConcept(
          name,
          genericParams.map(_._1._2).getOrElse(Nil),
          parentConcepts,
          withExpr.getOrElse(Nil),
          withoutExpr.getOrElse(Nil),
          whereExpr,
          orderByExpr.getOrElse(Nil),
          limitExpr.map(_.value),
          offsetExpr.map(_.value),
          annotations
        )
    }

  def cubeConceptDefinition: Parser[CubeConcept] =
    repsep(annotationParser, ",") ~
      conceptCubeKeyword ~
      sensIdent ~
      metricsKeyword ~ "(" ~ rep1sep(attributeParser, ",") ~ ")" ~
      opt(dimensionsKeyword ~> "(" ~ rep1sep(attributeParser, ",") ~ ")") ~
      fromKeyword ~ rep1sep(parentConceptParser, ",") ~
      opt(whereKeyword ~> sensExpression) ~
      opt(havingKeyword ~> sensExpression) ~
      opt(orderByKeyword ~> rep1sep(orderParser, ",")) ~
      opt(limitKeyword ~> sensIntLiteral) ~
      opt(offsetKeyword ~> sensIntLiteral) ^^ {
      case
        annotations ~
          conceptKvd ~ name ~
          metricsKvd ~ "(" ~ metrics ~ ")" ~
          dimensionsExpr ~
          "from" ~ parentConcepts ~
          whereExpr ~
          havingExpr ~
          orderByExpr ~
          limitExpr ~ offsetExpr => {
        val dimensions = if (dimensionsExpr.isDefined) {
          dimensionsExpr.get._1._2
        } else {
          Nil
        }
        CubeConcept(
          name,
          metrics,
          dimensions,
          parentConcepts,
          whereExpr,
          havingExpr,
          orderByExpr.getOrElse(Nil),
          limitExpr.map(_.value),
          offsetExpr.map(_.value),
          annotations
        )
      }
    }

  def cubeInheritedConceptDefinition: Parser[CubeInheritedConcept] =
    repsep(annotationParser, ",") ~
      conceptCubeKeyword ~ sensIdent ~
      isKeyword ~ parentConceptParser ~
      opt(withKeyword ~ metricsKeyword ~ rep1sep(attributeParser, ",")) ~
      opt(withoutKeyword ~ metricsKeyword ~ rep1sep(conceptAttribute, ",")) ~
      opt(withKeyword ~ dimensionsKeyword ~ rep1sep(attributeParser, ",")) ~
      opt(withoutKeyword ~ dimensionsKeyword ~ rep1sep(conceptAttribute, ",")) ~
      opt(whereKeyword ~> sensExpression) ~
      opt(havingKeyword ~> sensExpression) ~
      opt(orderByKeyword ~> rep1sep(orderParser, ",")) ~
      opt(limitKeyword ~> sensIntLiteral) ~
      opt(offsetKeyword ~> sensIntLiteral) ^^ {
      case
        annotations ~
          conceptKvd ~ name ~
          "is" ~ parentConcept ~
          withMetricsExpr ~
          withoutMetricsExpr ~
          withMDimensionsExpr ~
          withoutDimensionsExpr ~
          whereExpr ~
          havingExpr ~
          orderByExpr ~
          limitExpr ~ offsetExpr =>
        CubeInheritedConcept(
          name,
          parentConcept,
          withMetricsExpr.map(_._2).getOrElse(Nil),
          withoutMetricsExpr.map(_._2).getOrElse(Nil),
          withMDimensionsExpr.map(_._2).getOrElse(Nil),
          withoutDimensionsExpr.map(_._2).getOrElse(Nil),
          whereExpr,
          havingExpr,
          orderByExpr.getOrElse(Nil),
          limitExpr.map(_.value),
          offsetExpr.map(_.value),
          annotations
        )
    }

  def relationshipConceptDefinition: Parser[AggregationConcept] =
    repsep(annotationParser, ",") ~
    conceptKeyword ~
    sensIdent ~
    betweenKeyword ~ rep1sep(parentConceptParser, ",") ~
    opt(whereKeyword ~> sensExpression) ^^ {
      case
        annotations ~
        "concept" ~ name ~
        "between" ~ parentConcepts ~
        whereExpr =>
        AggregationConcept(
          name,
          parentConcepts,
          whereExpr,
          annotations
        )
    }

  def functionConceptDefinition: Parser[FunctionConcept] =
    repsep(annotationParser, ",") ~
    conceptKeyword ~
    sensIdent ~
    opt("(" ~> rep1sep(attributeParser, ",") <~ ")") ~
    opt(fromKeyword ~> rep1sep(parentConceptParser, ",")) ~
    byKeyword ~ sensFunction ^^ {
      case
        annotations ~
        "concept" ~ name ~
        attributes ~
        parentConcepts ~
        "by" ~ function =>
        FunctionConcept(
          name,
          attributes.getOrElse(List()),
          parentConcepts.getOrElse(List()),
          function,
          annotations
        )
    }

  def dataSourceParser: Parser[DataSource] = fileDataSourceParser

  def fileDataSourceParser: Parser[DataSource] = sensIdent ~ "file" ~ stringLiteral ^^ {value =>
    FileDataSource(removeQuotes(value._2), FileFormats.withName(value._1._1))
  }

  def dataSourceConceptDefinition: Parser[DataSourceConcept] =
    repsep(annotationParser, ",") ~
      dataSourceKeyword ~
      sensIdent ~
      "(" ~ rep1sep(attributeParser, ",") ~ ")" ~
      fromKeyword ~ dataSourceParser ^^ {
      case
        annotations ~
        "datasource" ~ name ~
         "(" ~ attributes ~ ")"  ~
        "from" ~ source =>
          DataSourceConcept(name, attributes, source, annotations)
    }

  def unionConceptDefinition: Parser[UnionConcept] =
    repsep(annotationParser, ",") ~
    conceptKeyword ~
    sensIdent ~
    unionKeyword ~ rep1sep(parentConceptParser, ",") ^^ {
      case
        annotations ~
        "concept" ~ name ~
        unionKeywordValue ~ parentConcepts =>
        UnionConcept(
          name,
          parentConcepts,
          annotations
        )
    }

  def intersectConceptDefinition: Parser[IntersectConcept] =
    repsep(annotationParser, ",") ~
      conceptKeyword ~
      sensIdent ~
      intersectKeyword ~ rep1sep(parentConceptParser, ",") ^^ {
      case
        annotations ~
          "concept" ~ name ~
          intersectKeywordValue ~ parentConcepts =>
        IntersectConcept(
          name,
          parentConcepts,
          annotations
        )
    }

  def minusConceptDefinition: Parser[MinusConcept] =
    repsep(annotationParser, ",") ~
      conceptKeyword ~
      sensIdent ~
      minusKeyword ~ rep1sep(parentConceptParser, ",") ^^ {
      case
        annotations ~
          "concept" ~ name ~
          minusKeywordValue ~ parentConcepts =>
        MinusConcept(
          name,
          parentConcepts,
          annotations
        )
    }

  def rangeExpression: Parser[Option[SensExpression]] = sensExpression ^^ { value =>
    if (value == NamedElementPlaceholder("unbounded")) {
      None
    } else {
      Some(value)
    }
  }

  def windowFunctionExpression: Parser[WindowFunction] =
    sensIdent ~ "(" ~ repsep(sensExpression, ",") ~ ")" ~
      overKeyword ~ "(" ~
      opt(partitionByKeyword ~ "(" ~ repsep(sensExpression, ",") ~ ")") ~
      opt(orderByKeyword ~ "(" ~ repsep(orderParser, ",") ~ ")") ~
      opt(rowsBetweenKeyword ~ "(" ~ rangeExpression ~ "," ~ rangeExpression ~ ")") ~
      opt(rangeBetweenKeyword ~ "(" ~ rangeExpression ~ "," ~ rangeExpression ~ ")") ~
      ")" ^^ {
      case functionName ~ "(" ~ arguments ~ ")" ~ overKeyword ~ "(" ~ partitionByExpr ~ orderByExpr ~ rowsBetweenExpr ~ rangeBetweenExpr ~ ")" =>
        WindowFunction(
          WindowFunctions.withNameUnknown(functionName),
          arguments,
          partitionByExpr.map(_._1._2).getOrElse(Nil),
          orderByExpr.map(_._1._2).getOrElse(Nil),
          extractRangeOpt(rowsBetweenExpr),
          extractRangeOpt(rangeBetweenExpr)
        )
    }

  def extractRangeOpt(rangeOpt: Option[String ~ String ~ Option[SensExpression] ~ String ~ Option[SensExpression] ~ String]
                     ): (Option[SensExpression],Option[SensExpression]) = {
    if(rangeOpt.isEmpty) {
      (None, None)
    } else {
      val rangeBase = rangeOpt.get._1
      (rangeBase._1._1._2 ,rangeBase._2)
    }
  }

}
