package org.sens.parser

import org.sens.converter.rel.StandardSensFunctions
import org.sens.core.SensElement
import org.sens.core.concept.{Attribute, ParentConcept, SensConcept}
import org.sens.core.expression.concept.{GenericConceptReference, SensConceptExpression}
import org.sens.core.statement.{ConceptDefinition, FunctionDefinition, SensFunctionDefinition, StandardSensStatements, VariableDefinition}
import org.sens.parser.FirstClassCitizens.FirstClassCitizen

import scala.collection.mutable

class ValidationContext(val frameStack: List[ValidationFrame]) {

  def addFrame: ValidationContext = new ValidationContext(new ValidationFrame :: frameStack)

  def addVariable(definition: VariableDefinition): Unit =
    add(definition.name, definition, (frame: ValidationFrame) => frame.variables)

  def addVariables(definitions: List[VariableDefinition]): Unit = {
    definitions.foreach(addVariable)
  }

  def getVariable(name: String): Option[VariableDefinition] =
    get(name, (frame: ValidationFrame) => frame.variables)

  def containsVariable(name: String): Boolean =
    contains(name, (frame: ValidationFrame) => frame.variables)

  def addFunction(definition: FunctionDefinition): Unit =
    add(definition.name, definition, (frame: ValidationFrame) => frame.userDefinedFunctions)

  def getFunction(name: String): Option[SensFunctionDefinition] = {
    val userDefinedFunction = get(name, (frame: ValidationFrame) => frame.userDefinedFunctions)
    if(userDefinedFunction.isDefined) {
      userDefinedFunction
    } else {
      StandardSensStatements.functions.get(name)
    }
  }

  def containsFunction(name: String): Boolean =
    contains(name, (frame: ValidationFrame) => frame.userDefinedFunctions) || StandardSensStatements.functions.contains(name)

  def getUserDefinedFunctions: List[FunctionDefinition] =
    frameStack.flatMap(_.userDefinedFunctions.values).distinct

  def addConcept(definition: ConceptDefinition): Unit =
    add(definition.concept.getName, definition, (frame: ValidationFrame) => frame.concepts)

  def getConcept(name: String): Option[ConceptDefinition] =
    get(name, (frame: ValidationFrame) => frame.concepts)

  def containsConcept(name: String): Boolean =
    contains(name, (frame: ValidationFrame) => frame.concepts)

  def getConcepts: List[ConceptDefinition] =
    frameStack.flatMap(_.concepts.values).distinct

  def setCurrentConcept(definition: SensConcept): Unit =
    frameStack.head.currentConcept = Some(definition)

  def loopCurrentConcepts(f: SensConcept => Option[Tuple2[FirstClassCitizen, List[SensElement]]]): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    var isNestedDefinition = true
    var i = 0
    while (i < frameStack.size && isNestedDefinition) {
      if (frameStack(i).currentConcept.isDefined) {
        val res = f(frameStack(i).currentConcept.get)
        if(res.isDefined) {
          return res
        }
        isNestedDefinition = frameStack(i).currentConcept.get.isInstanceOf[SensConceptExpression]
      }
      i += 1
    }
    None
  }

  def findAttribute(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    val attributeElement = loopCurrentConcepts(curConcept => {
      val attributes = curConcept.getCurrentAttributes(this).find(_.name == name)
      if (attributes.isDefined) {
        Some(Tuple2(FirstClassCitizens.Attribute, attributes.get :: Nil))
      } else {
        None
      }
    })
    attributeElement
  }

  def containsAttribute(name: String): Boolean = {
    findAttribute(name).isDefined
  }

  def findParentConcept(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    val parentConceptElement = loopCurrentConcepts(curConcept => {
      val parentConcept = curConcept.getParentConcepts(this).find(_.checkName(name))
      if (parentConcept.isDefined) {
        Some(Tuple2(FirstClassCitizens.ParentConcept, parentConcept.get :: Nil))
      } else {
        None
      }
    })
    parentConceptElement
  }

  def getParentConcept(name: String): Option[ParentConcept] = {
    findParentConcept(name).map(_._2.head.asInstanceOf[ParentConcept])
  }

  def containsParentConcept(name: String): Boolean = {
    findParentConcept(name).isDefined
  }

  def findGenericParameter(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    val genericParameters = loopCurrentConcepts(curConcept => {
      val genericParameters = curConcept.getGenericParameters.filter(_ == name)
      if (genericParameters.nonEmpty) {
        Some(Tuple2(
          FirstClassCitizens.GenericParameter,
          genericParameters.map(org.sens.core.expression.GenericParameter(_))))
      } else {
        None
      }
    })
    genericParameters
  }

  def containsGenericParameter(name: String): Boolean = {
    findGenericParameter(name).isDefined
  }

  def findParentConceptAttribute(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    val parentConceptAttributes = loopCurrentConcepts(curConcept => {
      val parentConcepts = curConcept.getParentConcepts(this)
      val parentConceptsAttributes = parentConcepts.filter(
        _.concept.getCurrentAttributes(this).exists(_.name == name)
      )
      if (parentConceptsAttributes.nonEmpty) {
        Some(Tuple2(FirstClassCitizens.ParentConceptAttribute, parentConceptsAttributes))
      } else {
        None
      }
    })
    parentConceptAttributes
  }

  def containsParentConceptAttribute(name: String): Boolean = {
    val parentConceptAttributes = findParentConceptAttribute(name)
    parentConceptAttributes.isDefined && parentConceptAttributes.get._2.size == 1
  }

  def findGenericParentConceptAttribute(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    val genericParentConceptAttributes = loopCurrentConcepts(curConcept => {
      val parentConcepts = curConcept.getParentConcepts(this)
      val genericParentConceptsAttributes = parentConcepts.filter(
        _.concept.isInstanceOf[GenericConceptReference]
      )
      if (genericParentConceptsAttributes.nonEmpty) {
        Some(Tuple2(FirstClassCitizens.GenericParentConceptAttribute, Nil))
      } else {
        None
      }
    })
    genericParentConceptAttributes
  }

  def get[T](name: String, containerGetter: ValidationFrame => mutable.Map[String, T]): Option[T] = {
    for(frame <- frameStack) {
      val curDefinition =  containerGetter.apply(frame).get(name)
      if(curDefinition.isDefined) {
        return curDefinition
      }
    }
    None
  }

  def add[T](name: String, definition: T, containerGetter: ValidationFrame => mutable.Map[String, T]): Unit = {
    containerGetter.apply(frameStack.head).put(name, definition)
  }

  def contains[T](name: String, containerGetter: ValidationFrame => mutable.Map[String, T]): Boolean = {
    for(frame <- frameStack) {
      val curDefinition =  containerGetter.apply(frame).get(name)
      if(curDefinition.isDefined) {
        return true
      }
    }
    false
  }

  def find(name: String): Option[Tuple2[FirstClassCitizen, List[SensElement]]] = {
    //Check current concept for attributes, parent concepts and generic parameters first
    findAttribute(name)
      .orElse(findParentConcept(name))
      .orElse(findGenericParameter(name))
      .orElse(findParentConceptAttribute(name))
    //Then check if it is a concept, variable or user defined function
      .orElse(getVariable(name).map(item => Tuple2(FirstClassCitizens.Variable, item :: Nil)))
      .orElse(getFunction(name).map(item => Tuple2(FirstClassCitizens.Function, item :: Nil)))
      .orElse(getConcept(name).map(item => Tuple2(FirstClassCitizens.Concept, item :: Nil)))
    //Check standard functions
      .orElse(StandardSensStatements.functions.get(name).map(item => Tuple2(FirstClassCitizens.Function, item :: Nil)))
    //Last, check if some of the parent concepts is generic
      .orElse(findGenericParentConceptAttribute(name))
  }
}

object ValidationContext {
  def apply(): ValidationContext = new ValidationContext(new ValidationFrame :: Nil)
}

class ValidationFrame {
  val variables = mutable.Map[String, VariableDefinition]()
  val userDefinedFunctions = mutable.Map[String, FunctionDefinition]()
  val concepts = mutable.Map[String, ConceptDefinition]()
  var currentConcept: Option[SensConcept] = None
}

object FirstClassCitizens extends Enumeration {
  type FirstClassCitizen = Value

  val Variable, Function, StandardSensFunction, Concept, Attribute, ParentConcept, ParentConceptAttribute,
      GenericParameter, GenericParentConceptAttribute = Value
}
