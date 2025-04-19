package org.sens.core.concept

import org.sens.core.expression.{ConceptAttribute, ConceptObject, GenericParameter, NamedElementPlaceholder, SensExpression, Variable}
import org.sens.core.statement.ConceptDefinition
import org.sens.parser.{ElementNotFoundException, GenericDefinitionException, ValidationContext, WrongConceptType}

import scala.util.{Failure, Success, Try}

case class ConceptAttributesReference(conceptName: SensIdent, parameters: Map[String, SensExpression]) extends SensAttribute {

  override def toSensString: String =
    conceptName.toSensString +
      (if (parameters.nonEmpty) "[" + parameters.map(item => item._1 + ": " + item._2.toSensString).mkString(", ") + "]" else "")

  def nameDefined: Boolean = {
    conceptName.findSubExpression {
      case _: GenericParameter => true
      case _: NamedElementPlaceholder => true
      case _: ConceptAttribute => true
      case _: ConceptObject => true
      case _: Variable => true
      case _ => false
    }.isEmpty
  }

  def containsGenericParameter: Boolean = this.findSubExpression {
    case _: GenericParameter => true
    case _: NamedElementPlaceholder => true
    case _ => false
  }.isDefined

  def getName: String = if (nameDefined) {
    conceptName.getName
  } else {
    throw GenericDefinitionException("Concept Name is not defined")
  }

  def getInstanceName: String = if (parameters.isEmpty) {
    getName
  } else {
    "_generic_" + getName + "_" + parameters.values.map(_.evaluate.get.stringValue).mkString("_")
  }

  def getAttributeNames(context: ValidationContext): List[String] = {
    if (containsGenericParameter) {
      Nil
    } else {
      getAttributesFromReference(conceptName, parameters, context).get.map(_.name)
    }
  }

  def getAttributes(context: ValidationContext): List[Attribute] = {
    if (containsGenericParameter) {
      Nil
    } else {
      getAttributesFromReference(conceptName, parameters, context).get
    }
  }

  def getCurrentAttributes(context: ValidationContext): List[Attribute] = {
    Nil
  }

  override def getSubExpressions: List[SensExpression] = parameters.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    conceptName.findSubExpression(f).orElse(
      collectFirstSubExpression(parameters.values.toList, f)
    )

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] =
    conceptName.findAllSubExpressions(f) ::: parameters.values.flatMap(_.findAllSubExpressions(f)).toList


  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): ConceptAttributesReference =
    ConceptAttributesReference(
      conceptName.replaceSubExpression(replaceSubExpression, withSubExpression),
      parameters.mapValues(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ConceptAttributesReference] = {
    val validatedResult = Try(
      ConceptAttributesReference(
        conceptName.validateAndRemoveVariablePlaceholders(context).get,
        parameters.map(item => item._1 -> item._2.validateAndRemoveVariablePlaceholders(context).get)
      ))
    validatedResult match {
      case Failure(_) => return validatedResult
      case Success(validated) =>
        if (nameDefined) {
          val instantiatedAttributes = getAttributesFromReference(validated.conceptName, validated.parameters, context)
          instantiatedAttributes match {
            case Failure(err) => return Failure(err)
            case Success(_) => return validatedResult
          }
        }
    }
    validatedResult
  }

  def getAttributesFromReference(conceptRef: SensIdent, parametersMap: Map[String, SensExpression], context: ValidationContext): Try[List[Attribute]] = {
    if (!context.containsConcept(conceptRef.getName)) {
      return Failure(ElementNotFoundException(conceptRef.getName))
    }
    val conceptDef = context.getConcept(conceptRef.getName).get.concept
    if (!conceptDef.isInstanceOf[ConceptAttributes]) {
      return Failure(WrongConceptType("ConceptAttributes", conceptDef.getClass.getSimpleName))
    }
    val conceptAttributesInstance = conceptDef.asInstanceOf[ConceptAttributes]
    val parentConceptAliases = conceptAttributesInstance.getParentConceptAliases
    val attributesList = conceptAttributesInstance.getAttributes(context)
    val genericParameters = attributesList.flatMap(_.findAllSubExpressions {
      case _: GenericParameter => true
      case _ => false
    }).distinct.map(_.asInstanceOf[GenericParameter].name)
    if (genericParameters.size + parentConceptAliases.size > parametersMap.size ||
      genericParameters.exists(!parametersMap.contains(_)) ||
      parentConceptAliases.exists(!parametersMap.contains(_))
    ) {
      return Failure(GenericDefinitionException("Generic parameters do not match for concept " + getName))
    }
    val parametersDefined = parametersMap.values.flatMap(_.findSubExpression {
      case _: GenericParameter => true
      case _: NamedElementPlaceholder => true
      case _ => false
    }).isEmpty
    if (parametersDefined) {
      val instantiatedAttributes = instantiateAttributes(
        attributesList,
        genericParameters,
        parentConceptAliases,
        parametersMap,
        conceptAttributesInstance,
        context)
      return Success(instantiatedAttributes)
    }
    Success(Nil)
  }

  def instantiateAttributes(attributesList: List[Attribute],
                            genericParameterNames: List[String],
                            aliases: List[String],
                            parameters: Map[String, SensExpression],
                            conceptAttributes: SensConcept,
                            context: ValidationContext
                           ): List[Attribute] = {
    val instantiatedAttributes = attributesList.map(attr => {
      var instantiatedAttribute = attr
      for ((curParamName, curParamValue) <- parameters) {
        if (genericParameterNames.contains(curParamName)) {
          instantiatedAttribute = instantiatedAttribute.replaceSubExpression(GenericParameter(curParamName), curParamValue)
        } else {
          instantiatedAttribute = replaceAlias(instantiatedAttribute, aliases, conceptAttributes, context)
        }
      }
      instantiatedAttribute
    })
    instantiatedAttributes
  }

  def replaceAlias(attribute: Attribute, aliases: List[String], conceptAttributes: SensConcept, context: ValidationContext): Attribute = {
    var attributeUpdated = attribute
    aliases.foreach(curAlias => {
      val expressionsToReplace = attribute.findAllSubExpressions {
        case ConceptAttribute(conceptsChain, _) if conceptsChain.nonEmpty && conceptsChain.head == curAlias => true
        case _ => false
      }
      val aliasValue = parameters(curAlias).evaluate.get.stringValue
      expressionsToReplace.foreach(curExpr => {
        val curConceptAttribute = curExpr.asInstanceOf[ConceptAttribute]
        val newExpr = ConceptAttribute(aliasValue :: curConceptAttribute.conceptsChain.tail, curConceptAttribute.attribute)
        attributeUpdated = attributeUpdated.replaceSubExpression(curExpr, newExpr)
      })

      val expressionsWithoutAliasesToReplace = attribute.findAllSubExpressions {
        case ConceptAttribute(Nil, attrName) if isAttributeBelongToParentConcept(attrName, curAlias, conceptAttributes, context) => true
        case _ => false
      }
      expressionsWithoutAliasesToReplace.foreach(curExpr => {
        val curConceptAttribute = curExpr.asInstanceOf[ConceptAttribute]
        val newExpr = ConceptAttribute(aliasValue :: Nil, curConceptAttribute.attribute)
        attributeUpdated = attributeUpdated.replaceSubExpression(curExpr, newExpr)
      })
    })

    attributeUpdated
  }

  def isAttributeBelongToParentConcept(attrName: String, parentConceptName: String, conceptAttributes: SensConcept, context: ValidationContext): Boolean = {
    val attrs = conceptAttributes.getParentConcepts(context).zipWithIndex.filter(curItem => {
      val (currentParentConcept, index) = curItem
      currentParentConcept.alias.isEmpty && index.toString == parentConceptName &&
        (currentParentConcept.concept.getCurrentAttributes(context).exists(_.name == attrName) ||
          conceptAttributes.isGeneric && conceptAttributes.getParentConcepts(context).size == 1)
    })
    attrs.size == 1
  }
}
