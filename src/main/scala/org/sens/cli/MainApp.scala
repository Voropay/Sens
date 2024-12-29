package org.sens.cli

import org.sens.converter.rel.{ConceptSqlComposer, ConceptSqlMaterializer, Utils}
import org.sens.core.DataModelElement
import org.sens.core.concept.{Annotation, Concept, ParentConcept, SensConcept}
import org.sens.core.expression.SensExpression
import org.sens.core.expression.concept.{AnonymousConceptDefinition, ConceptReference}
import org.sens.core.statement.{ConceptDefinition, FunctionDefinition}
import org.sens.parser.{DataModelParserError, ParsingException, SensParser, ValidationContext}

import scala.util.{Failure, Success}

object MainApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Please specify command and path to a data model");
      return;
    }
    val dataModelPath = args(1);
    val command = args(0);

    val dataModelSource = scala.io.Source.fromFile(dataModelPath)
    val dataModelStr = try dataModelSource.mkString finally dataModelSource.close()

    val conceptParser = new SensParser
    val dataModelRes = parseDataModel(dataModelStr, conceptParser)
    dataModelRes match {
      case Left(errors) =>
        println("Data model validation failed")
        errors.foreach(println(_))
      case Right(context) =>
        executeCommand(command, context, conceptParser, args)
    }
  }

  def getFullRefreshArgument(args: Array[String]): Boolean = {
    if (args.size >= 3 && args(2).nonEmpty) {
      args(2).toLowerCase == "true"
    } else {
      true
    }
  }

  def getValueArgument(args: Array[String], conceptParser: SensParser): Either[DataModelParserError, Option[SensExpression]] = {
    if (args.size >= 4 && args(3).nonEmpty) {
      conceptParser.parseExpression(args(3)) match {
        case Left(error) => Left(error)
        case Right(res) => Right(Some(res))
      }
    } else {
      Right(None)
    }
  }

  def executeCommand(command: String, context: ValidationContext, conceptParser: SensParser, args: Array[String]): Unit = {
    command match {
      case "validate" =>
        println("Data model validation successfully completed")
      case "materialize" =>
        val fullRefresh = getFullRefreshArgument(args)
        val valueRes = getValueArgument(args, conceptParser)
        valueRes match {
          case Left(err) =>
            println("Incorrect command argument: ")
            println(err)
          case Right(value) =>
            val sqlCode = materialize(context, fullRefresh, value)
            println(sqlCode)
        }
      case "query" =>
        queryLoop(context, conceptParser)
    }
  }

  def validateDataModel(dataModel: List[DataModelElement]): Either[List[Throwable], ValidationContext] = {
    val context = ValidationContext()
    dataModel.foreach({
      case c: ConceptDefinition => context.addConcept(c)
      case f: FunctionDefinition => context.addFunction(f)
    })
    val validatedDataModel: List[Either[Throwable, DataModelElement]] = dataModel.map(el => {
      val validatedElement: Either[Throwable, DataModelElement] = el match {
        case c: ConceptDefinition =>
          c.concept.validateAndRemoveVariablePlaceholders(context) match {
            case Success(vc) =>
              context.addConcept(ConceptDefinition(vc))
              Right(ConceptDefinition(vc))
            case Failure(exception) => Left(exception)
          }
        case f: FunctionDefinition => Right(f)
      }
      validatedElement
    })

    val errors = validatedDataModel.filter(_.isLeft)
    if(errors.nonEmpty) {
      Left(errors.map(_.left.get))
    } else {
      validatedDataModel.foreach({
        case Right(el) => el match {
          case c: ConceptDefinition => context.addConcept(c)
          case f: FunctionDefinition => context.addFunction(f)
        }
      })
      Right(context)
    }
  }

  def parseDataModel(dataModelStr: String, conceptParser: SensParser): Either[List[Throwable], ValidationContext] = {
    val parsingResults = conceptParser.parseDataModel(dataModelStr)

    parsingResults match {
      case Left(errorMsg) =>
        Left(ParsingException(errorMsg.msg) :: Nil)
      case Right(dataModel) =>
        validateDataModel(dataModel)
    }
  }

  def instantiateConceptsToMaterialize(context: ValidationContext): Either[DataModelParserError, ValidationContext] = {
    val contextToMaterialize = ValidationContext()
    val conceptDefs = context.getConcepts.filter(_.concept.getMaterialization != Annotation.MATERIALIZED_EPHEMERAL)
    conceptDefs.foreach(curConDef => {
      val instantiationError = instantiateConcept(curConDef.concept, context, contextToMaterialize)
      if(instantiationError.isDefined) {
        return Left(instantiationError.get)
      }
    })
    Right(contextToMaterialize)
  }

  def instantiateConcept(concept: SensConcept, sourceContext: ValidationContext, targetContext: ValidationContext): Option[DataModelParserError] = {
    val conceptInstanceRes = concept.inferAttributeExpressions(sourceContext)
    conceptInstanceRes match {
      case Failure(error) =>
        Some(DataModelParserError(error.getMessage))
      case Success(conceptInstance) =>
        targetContext.addConcept(ConceptDefinition(conceptInstance))
        val parentConceptRefs = conceptInstance.findAllSubExpressions({
          case _: ConceptReference => true
          case _ => false
        })
        parentConceptRefs.foreach(pc => {
          val conceptName = pc.asInstanceOf[ConceptReference].conceptName
          val parentConDef = sourceContext.getConcept(conceptName)
          if(parentConDef.isEmpty) {
            return Some(DataModelParserError("Concept %s not found".format(conceptName)))
          }
          val parentConceptInstanceRes = instantiateConcept(parentConDef.get.concept, sourceContext, targetContext)
          if(parentConceptInstanceRes.isDefined) {
            return parentConceptInstanceRes
          }
        })
        None
    }
  }

  def materialize(context: ValidationContext, fullRefresh: Boolean = true, value: Option[SensExpression]= None): String = {
    val contextToMaterializeRes = instantiateConceptsToMaterialize(context)
    contextToMaterializeRes match {
      case Left(error) => error.msg
      case Right(contextToMaterialize) =>
        val materializer = ConceptSqlMaterializer.create(contextToMaterialize)
        val concepts = conceptNamesMap(contextToMaterialize)
        val dependencies = dependenciesGraph(concepts)
        val orderedConceptNames = Utils.tsort(dependencies, Seq())
        var result: List[String] = List()
        orderedConceptNames.foreach(conceptName => {
          val concept = concepts(conceptName)
          val conceptSql = materializer.materializeSql(concept, fullRefresh, value)
          if (!conceptSql.isBlank) {
            result = conceptSql :: result
          }
        })
        result.reverse.mkString("\n")
    }
  }

  def conceptNamesMap(context: ValidationContext): Map[String, SensConcept] = {
    val conceptDefs = context.getConcepts
    conceptDefs.map(cd =>
      (cd.concept.getName, cd.concept)
    ).toMap
  }

  def dependenciesGraph(concepts: Map[String, SensConcept]): Map[String, Set[String]] = {
    concepts.map(element => {
      val edge = element._1
      val parentConceptRefs = element._2.findAllSubExpressions {
        case _: ConceptReference => true
        case _ => false
      }
      val parentConceptLinks = parentConceptRefs.map {
        case ref: ConceptReference => ref.conceptName
      }
      (edge, parentConceptLinks.toSet)
    })
  }

  def queryLoop(context: ValidationContext, conceptParser: SensParser): Unit = {
    val composer = ConceptSqlComposer.create(context)
    println("enter query:")
    var queryStr = scala.io.StdIn.readLine()
    while (queryStr != "exit") {
      val querySql = queryToSql(queryStr, context, conceptParser, composer)
      querySql match {
        case Left(err) => println(err)
        case Right(sql) => println(sql)
      }
      println("enter query:")
      queryStr = scala.io.StdIn.readLine()
    }
  }

  def validateQuery(query: AnonymousConceptDefinition, context: ValidationContext): Either[Throwable, SensConcept] = {
    val conceptDefinition = query.toConcept()
    conceptDefinition.validateAndRemoveVariablePlaceholders(context).flatMap(_.inferAttributeExpressions(context)) match {
      case Success(vc) => Right(vc)
      case Failure(exception) => Left(exception)
    }
  }

  def queryToSql(queryStr: String, context: ValidationContext, conceptParser: SensParser, composer: ConceptSqlComposer): Either[String, String] = {
    val parsingResult = conceptParser.parseQuery(queryStr)
    parsingResult match {
      case Left(errorMsg) =>
        Left("Parsing query error: " + errorMsg.msg)
      case Right(queryDef) =>
        val validationResult = validateQuery(queryDef, context)
        validationResult match {
          case Left(errorMsg) =>
            Left("Validation query error: " + errorMsg)
          case Right(conDef) =>
            Right(composer.composeSelect(conDef))
        }
    }
  }
}
