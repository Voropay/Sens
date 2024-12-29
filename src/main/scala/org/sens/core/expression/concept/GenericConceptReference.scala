package org.sens.core.expression.concept

import org.sens.core.concept.{Annotation, Attribute, ParentConcept, SensConcept, SensIdent}
import org.sens.core.expression.literal.SensLiteral

import scala.util.{Failure, Success, Try}
import org.sens.core.expression.{ConceptAttribute, ConceptObject, GenericParameter, NamedElementPlaceholder, SensExpression, Variable}
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.{ElementNotFoundException, GenericDefinitionException, ValidationContext, WrongTypeException}

case class GenericConceptReference(conceptName: SensIdent, parameters: Map[String, SensExpression]) extends SensConceptExpression {

  override def nameDefined: Boolean = {
    conceptName.findSubExpression {
      case _: GenericParameter => true
      case _: NamedElementPlaceholder => true
      case _: ConceptAttribute => true
      case _: ConceptObject => true
      case _: Variable => true
      case _ => false
    }.isEmpty
  }

  override def getName: String = if(nameDefined) {
    conceptName.getName
  } else {
    throw GenericDefinitionException("Concept Name is not defined")
  }

  override def isGeneric: Boolean = true

  override def getInstanceName: String = if(parameters.isEmpty) {
    getName
  } else {
    "_generic_" + getName + "_" + parameters.values.map(_.evaluate.get.stringValue).mkString("_")
  }

  override def getGenericParameters: List[String] = parameters.keys.toList

  override def getInstance(context: ValidationContext, name: String, genericParameterValues: Map[String, SensLiteral]): SensConcept =
    getGenericConceptInstance(context, genericParameterValues)

  def containsGenericParameter: Boolean = this.findSubExpression {
    case _: GenericParameter => true
    case _: NamedElementPlaceholder => true
    case _ => false
  }.isDefined

  def getGenericConceptInstance(context: ValidationContext, validatedParameters: Map[String, SensExpression]): SensConcept = {
    val genericConceptName = getName
    val instanceName = getInstanceName
    val conceptInstanceDefinition = context.getConcept(instanceName)
    if(conceptInstanceDefinition.isDefined) {
      conceptInstanceDefinition.get.concept
    } else {
      val genericConceptDefinition = context.getConcept(genericConceptName)
      if (genericConceptDefinition.isEmpty) {
        throw ElementNotFoundException(genericConceptName)
      }
      val parameterValues = validatedParameters.mapValues(_.evaluate).mapValues {
        case Failure(exception) => throw exception
        case Success(value) => value
      }
      val validatedGenericConceptDefinition = genericConceptDefinition.get.concept.validateAndRemoveVariablePlaceholders(context)
      val conceptInstance = validatedGenericConceptDefinition.get.getInstance(context, instanceName, parameterValues)
      val validatedInstance = conceptInstance.validateAndRemoveVariablePlaceholders(context)
      validatedInstance match {
        case Failure(exception) => throw exception
        case Success(instance) =>
          context.addConcept(ConceptDefinition(instance))
          instance
      }
    }
  }

  override def getAttributeNames(context: ValidationContext): List[String] = {
    if(containsGenericParameter) {
      Nil
    } else {
      val conceptInstance = getGenericConceptInstance(context, parameters)
      conceptInstance.getAttributeNames(context)
    }
  }

  override def getAttributes(context: ValidationContext): List[Attribute] = {
    if (containsGenericParameter) {
      Nil
    } else {
      val conceptInstance = getGenericConceptInstance(context, parameters)
      conceptInstance.getAttributes(context)
    }
  }

  override def getCurrentAttributes(context: ValidationContext): List[Attribute] = {
    if (containsGenericParameter) {
      Nil
    } else {
      val conceptInstance = getGenericConceptInstance(context, parameters)
      conceptInstance.getCurrentAttributes(context)
    }
  }

  override def getParentConcepts(context: ValidationContext): List[ParentConcept] = {
    if (containsGenericParameter) {
      Nil
    } else {
      val conceptInstance = getGenericConceptInstance(context, parameters)
      conceptInstance.getParentConcepts(context)
    }
  }

  override def getAnnotations: List[Annotation] = List()

  override def toSensString: String =
    conceptName.toSensString +
      (if(parameters.nonEmpty) "[" + parameters.map(item => item._1 + ": " + item._2.toSensString).mkString(", ") + "]" else "")

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConceptExpression] = {
    val validatedResult = Try(
      GenericConceptReference(
        conceptName.validateAndRemoveVariablePlaceholders(context).get,
        parameters.map(item => item._1 -> item._2.validateAndRemoveVariablePlaceholders(context).get)
    ))
    validatedResult match {
      case Failure(_) => return validatedResult
      case Success(validated) =>
        val areParametersDefined = validated.parameters.values.flatMap(_.findSubExpression {
          case _: GenericParameter => true
          case _: NamedElementPlaceholder => true
          case _ => false
        }).isEmpty
        if(nameDefined) {
          if (!context.containsConcept(validated.getName)) {
            return Failure(ElementNotFoundException(validated.getName))
          }
          val genericInstance = context.getConcept(validated.getName).get.concept
          if (genericInstance.getGenericParameters.size != validated.parameters.size ||
            genericInstance.getGenericParameters.exists(!validated.parameters.contains(_))
          ) {
            return Failure(GenericDefinitionException("Generic parameters do not match for concept " + getName))
          }
          if(areParametersDefined) {
            val conceptInstance = Try(getGenericConceptInstance(context, validated.parameters))
            conceptInstance match {
              case Failure(exception) => return Failure(exception)
              case Success(_) => return validatedResult
            }
          }
        }
      }
    validatedResult
  }

  override def inferAttributeExpressions(context: ValidationContext): Try[SensConceptExpression] = {
    val conceptInstance = Try(getGenericConceptInstance(context, parameters))
    conceptInstance match {
      case Failure(exception) => Failure(exception)
      case Success(value) => Success(ConceptReference(value.getName))
    }
  }

  override def getSubExpressions: List[SensExpression] = parameters.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      conceptName.findSubExpression(f).orElse(
        collectFirstSubExpression(parameters.values.toList, f)
      )
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: conceptName.findAllSubExpressions(f) ::: parameters.values.flatMap(_.findAllSubExpressions(f)).toList
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConceptExpression =
    if (this == replaceSubExpression) {
      withSubExpression match {
        case acd: SensConceptExpression => acd
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensConceptExpression")
      }
    } else {
      GenericConceptReference(
        conceptName.replaceSubExpression(replaceSubExpression, withSubExpression),
        parameters.mapValues(_.replaceSubExpression(replaceSubExpression, withSubExpression))
      )
    }
}
