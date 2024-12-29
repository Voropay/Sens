package org.sens.core.concept

import org.sens.core.expression.operation.comparison.Equals
import org.sens.core.expression.operation.logical.And
import org.sens.core.expression.{ConceptAttribute, SensExpression, Variable}
import org.sens.core.{DataModelElement, SensElement}
import org.sens.core.expression.concept.{AnonymousConceptDefinition, AnonymousFunctionConceptDefinition, ConceptReference, GenericConceptReference}
import org.sens.core.expression.literal.{SensLiteral, StringLiteral}
import org.sens.parser.{AttributeExpressionNotFound, ElementNotFoundException, ValidationContext}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

trait SensConcept extends SensElement {
  def getName: String
  def getInstanceName: String = getName
  def nameDefined: Boolean = true
  def getAttributeNames(context: ValidationContext): List[String]
  def getAttributes(context: ValidationContext): List[Attribute]
  def getCurrentAttributes(context: ValidationContext): List[Attribute]
  def getParentConcepts(context: ValidationContext): List[ParentConcept]
  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensConcept]
  def inferAttributeExpressions(context: ValidationContext): Try[SensConcept]
  def getAnnotations: List[Annotation]
  def getGenericParameters: List[String] = Nil
  def isGeneric: Boolean = getGenericParameters.nonEmpty
  def getInstance(context: ValidationContext, name: String, genericParameterValues: Map[String, SensLiteral]): SensConcept = this
  def isTransparent: Boolean
  def getMaterialization: Annotation = getAnnotations.find(_.name == Annotation.MATERIALIZED).getOrElse(Annotation.MATERIALIZED_EPHEMERAL)

  def expressionToAttribute(expression: SensExpression, attributesNames: Set[String]): Option[String] = {
    expression match {
      case expr: ConceptAttribute =>
        if(expr.conceptsChain.isEmpty && attributesNames.contains(expr.attribute)) {
          Some(expr.attribute)
        } else {
          None
        }
      case expr: Variable =>
        Some(expr.name).filter(attributesNames.contains)
      case _ => None
    }
  }

  def splitAndExpression(expression: SensExpression): List[SensExpression] = {
    expression match {
      case exp: And => {
        val leftOp = splitAndExpression(exp.operand1)
        val rightOp = splitAndExpression(exp.operand2)
        leftOp ::: rightOp
      }
      case _ => expression :: Nil
    }
  }

  def deriveAttributeExpressionsFromDependencies(attributesNames: Set[String], attributeDependencies: Option[SensExpression]): Map[String, SensExpression] = {
    var attributesMap: Map[String, SensExpression] = Map()
    if(attributeDependencies.isDefined) {
      val dependencies = splitAndExpression(attributeDependencies.get)
      for(curDep <- dependencies) {
        curDep match {
          case exp: Equals => {
            val leftAttrName = expressionToAttribute(exp.operand1, attributesNames)
            val rightAttrName = expressionToAttribute(exp.operand2, attributesNames)
            if(leftAttrName.isDefined && rightAttrName.isEmpty && !attributesMap.contains(leftAttrName.get)) {
              attributesMap += (leftAttrName.get -> exp.operand2)
            }
            if(rightAttrName.isDefined && leftAttrName.isEmpty && !attributesMap.contains(rightAttrName.get)) {
              attributesMap += (rightAttrName.get -> exp.operand1)
            }
            //ToDo: handle attributes equality eg. attr1 = attr2
          }
        }
      }
    }
    attributesMap
  }

  def equalAttributeNames(attributeNames: List[List[String]]): Boolean = {
    var curSet = attributeNames.head.toSet
    for(nextList<- attributeNames.tail) {
      if(curSet != nextList.toSet) {
        return false
      }
      curSet = nextList.toSet
    }
    true
  }

  def pickAttributeExpressionFromList(attribute: ConceptAttribute, attributeEqualityExpressions: ListBuffer[Equals]): Option[SensExpression] = {
    var i = 0
    while(i < attributeEqualityExpressions.size) {
      val curExpression = attributeEqualityExpressions(i)
      if(curExpression.operand1 == attribute) {
        val expression = curExpression.operand2
        attributeEqualityExpressions.remove(i)
        return Some(expression)
      }
      if(curExpression.operand2 == attribute) {
        val expression = curExpression.operand1
        attributeEqualityExpressions.remove(i)
        return Some(expression)
      }
      i += 1
    }
    None
  }

  def pickAllowedAttributeExpressionFromList(
                                              attribute: ConceptAttribute,
                                              attributeEqualityExpressions: ListBuffer[Equals],
                                              allowedConceptAttributes: List[ConceptAttribute]
                                            ): Option[SensExpression] = {
    var i = 0
    while(i < attributeEqualityExpressions.size) {
      val curExpression = attributeEqualityExpressions(i)
      if(curExpression.operand1 == attribute && allowedForJoin(curExpression.operand2, allowedConceptAttributes)) {
        val expression = curExpression.operand2
        attributeEqualityExpressions.remove(i)
        return Some(expression)
      }
      if(curExpression.operand2 == attribute && allowedForJoin(curExpression.operand1, allowedConceptAttributes)) {
        val expression = curExpression.operand1
        attributeEqualityExpressions.remove(i)
        return Some(expression)
      }
      i += 1
    }
    None
  }

  def allowedForJoin(expression: SensExpression, allowedConceptAttributes: List[ConceptAttribute]): Boolean = {
    expression match {
      case ca: ConceptAttribute => allowedConceptAttributes.contains(ca)
      case l: SensLiteral => true
      case _ => false
    }
  }

  def removeAttributeExpressionFromList(expression: Equals, attributeEqualityExpressions: ListBuffer[Equals]): Unit = {
    var i = 0
    while(i < attributeEqualityExpressions.size) {
      if(attributeEqualityExpressions(i) == expression) {
        attributeEqualityExpressions.remove(i)
        return
      } else {
        i += 1
      }
    }
  }

  def getExpressionsFromParentConcepts(attributeEqualityExpressions: ListBuffer[Equals], parentConcepts: List[ParentConcept]) {
    for(curConcept <- parentConcepts) {
      for(curAttrExpr <- curConcept.attributeDependencies) {
        val curParentAttribute = ConceptAttribute(curConcept.getInstanceAlias :: Nil, curAttrExpr._1)
        attributeEqualityExpressions += Equals(curParentAttribute, curAttrExpr._2)
      }
    }
  }

  def getExpressionsFromDependencies(
                                      attributeEqualityExpressions: ListBuffer[Equals],
                                      otherExpressions: ListBuffer[SensExpression],
                                      attributeDependencies: Option[SensExpression]
                                    ): Unit = {
    if(attributeDependencies.isDefined) {
      val dependencies = splitAndExpression(attributeDependencies.get)
      for(curDep <- dependencies) {
        if(curDep.isInstanceOf[Equals]) {
          val curEquals = curDep.asInstanceOf[Equals]
          if(curEquals.operand1.isInstanceOf[ConceptAttribute] || curEquals.operand2.isInstanceOf[ConceptAttribute]) {
            attributeEqualityExpressions += curEquals
          } else {
            otherExpressions += curDep
          }
        } else {
          otherExpressions += curDep
        }
      }
    }
  }

  def inferAttributesFromParentConceptsExpressions(curAttrName: String, parentConcepts: List[ParentConcept], context: ValidationContext): Option[SensExpression] = {
    val parentConceptsAttributes = parentConcepts.map(pc => {
      val parentAttributes = pc.concept.getAttributeNames(context)
      if(parentAttributes.contains(curAttrName)) {
        Some(ConceptAttribute(pc.getInstanceAlias :: Nil, curAttrName))
      } else {
        None
      }
    }).filter(_.isDefined)
    if(parentConceptsAttributes.size == 1) {
      parentConceptsAttributes.head
    } else {
      None
    }
  }

  def inferAttributesForParentConcepts(
                                        parentConcepts: List[ParentConcept],
                                        attributeEqualityExpressions: ListBuffer[Equals],
                                        context: ValidationContext
                                      ): List[Try[ParentConcept]] = {
    //Pick all expressions for FunctionConcept attributes annotated as Input because they must to be assigned values
    val functionConceptCallExpressions = pickExpressionsForFunctionConceptCall(parentConcepts, attributeEqualityExpressions, context)

    val headParentConcept = Try(ParentConcept(
      parentConcepts.head.concept.inferAttributeExpressions(context).get,
      parentConcepts.head.alias,
      functionConceptCallExpressions.getOrElse(parentConcepts.head.getInstanceAlias, Map()),
      parentConcepts.head.annotations
    ))
    var allowedConceptAttributes: List[ConceptAttribute] = parentConcepts.head.concept.getAttributeNames(context).map(curAttr =>
      ConceptAttribute(parentConcepts.head.getInstanceAlias :: Nil, curAttr)
    )
    val otherParentConcepts = parentConcepts.tail.map(curParentConcept => {
      val curAttributes = curParentConcept.concept.getAttributeNames(context)
      val allAttributeExpressions: List[(String, Option[SensExpression])] = curAttributes.map(curAttr => {
        //
        //Get expressions from FunctionConcept expressions if any or from parent concept definitions
        val curFuncExpression = functionConceptCallExpressions.getOrElse(curParentConcept.getInstanceAlias, Map()).get(curAttr)
        val curExpression = if(curFuncExpression.isDefined) {
          curFuncExpression
        } else {
          pickAllowedAttributeExpressionFromList(
            ConceptAttribute(curParentConcept.getInstanceAlias :: Nil, curAttr),
            attributeEqualityExpressions,
            allowedConceptAttributes
          )
        }
        (curAttr, curExpression)
      })

      allowedConceptAttributes = allowedConceptAttributes ::: curAttributes.map(curAttr =>
        ConceptAttribute(curParentConcept.getInstanceAlias :: Nil, curAttr)
      )

      val definedAttributeExpressions: Map[String, SensExpression] = allAttributeExpressions
        .filter(_._2.isDefined)
        .map(item => (item._1, item._2.get))
        .toMap

      val parentConceptDefinition = curParentConcept.concept match {
        case ref: ConceptReference => Success(ref)
        case genRef: GenericConceptReference => genRef.inferAttributeExpressions(context)
        case anonDef: AnonymousConceptDefinition => anonDef.inferAttributeExpressions(context)
        case anonFunc: AnonymousFunctionConceptDefinition => Success(anonFunc)
      }
      Try(ParentConcept(
        parentConceptDefinition.get,
        curParentConcept.alias,
        definedAttributeExpressions,
        curParentConcept.annotations
      ))
    })
    headParentConcept :: otherParentConcepts
  }

  def pickExpressionsForFunctionConceptCall(parentConcepts: List[ParentConcept],
                                            attributeEqualityExpressions: ListBuffer[Equals],
                                            context: ValidationContext): Map[String, Map[String, SensExpression]] = {
    val allConceptAttributes = parentConcepts.flatMap(curParentConcept => {
      val curAttributes = curParentConcept.concept.getAttributeNames(context)
      curAttributes.map(curAttr => ConceptAttribute(curParentConcept.getInstanceAlias :: Nil, curAttr))
    })
    parentConcepts.map(curParentConcept => {
      val argumentExpressions: Map[String, SensExpression] = curParentConcept.concept match {
        case conRef: ConceptReference => {
          val conDef = context.getConcept(conRef.getName)
          if(conDef.isEmpty) {
            throw ElementNotFoundException(conRef.getName)
          }
          conDef.get.concept match {
            case funcConDef: FunctionConcept => {
              val arguments = funcConDef.getInputAttributes.map(curAttr => ConceptAttribute(curParentConcept.getInstanceAlias :: Nil, curAttr.name))
              arguments.map(curArg => {
                val curExpression = pickAllowedAttributeExpressionFromList(
                  curArg,
                  attributeEqualityExpressions,
                  allConceptAttributes
                )
                if(curExpression.isEmpty) {
                  throw AttributeExpressionNotFound(curArg.attribute)
                }
                (curArg.attribute, curExpression.get)
              }).toMap
            }
            case _ => Map()
          }
        }
        case _ => Map()
      }
      (curParentConcept.getInstanceAlias, argumentExpressions)
    }).toMap
  }

  def joinAndExpressions(expressions: List[SensExpression]): Option[SensExpression] = {
    if(expressions.isEmpty) {
      None
    } else if(expressions.size == 1) {
      Some(expressions.head)
    } else {
      Some(And(expressions.head, joinAndExpressions(expressions.tail).get))
    }
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensConcept

  def getMaterializationName: String = {
    val materializationTargetName: Option[String] = getAnnotations
      .find(_.name == Annotation.MATERIALIZED)
      .map(_.attributes)
      .flatMap(_.get(Annotation.TARGET_NAME))
      .map({
        case n: StringLiteral => n.value
        case _ => throw new NotImplementedError()
      })
    materializationTargetName.getOrElse(this.getName)
  }

  def instantiateGenericConcepts(parentConcepts: List[ParentConcept], genericParameterValues: Map[String, SensLiteral], context: ValidationContext): List[ParentConcept] = {
    parentConcepts.map(pc => {
      val parentConceptInstance = pc.concept match {
        case gpc: GenericConceptReference =>
          val parentConceptDef = gpc.getGenericConceptInstance(context, gpc.parameters)
          ConceptReference(parentConceptDef.getName)
        case ac: AnonymousConceptDefinition =>
          ac.getInstance(context, "", genericParameterValues)
        case other => other
      }
      pc.copy(concept = parentConceptInstance)
    })
  }
}

