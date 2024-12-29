package org.sens.converter.optimization.concept

import org.sens.core.concept._
import org.sens.core.expression.ConceptAttribute
import org.sens.core.expression.concept.{AnonymousConceptDefinition, AnonymousFunctionConceptDefinition, ConceptReference, SensConceptExpression}
import org.sens.parser.ValidationContext

object RemoveUnusedAttributesRule {
  def apply(concept: SensConcept, childConcepts: List[SensConcept], context: ValidationContext): SensConcept = {
    var modifiedConcept = concept
    var attributesInUse: List[Attribute] = Nil
    concept.getAttributes(context).foreach(attr => {
      childConcepts.foreach(curChildConcept => {
        val parentConceptAliases = findAliases(curChildConcept.getParentConcepts(context), concept.getName, context)
        val conceptAttributes = parentConceptAliases.map(alias => ConceptAttribute(alias :: Nil, attr.name))
        if(conceptAttributes.exists(curConceptAttr => curChildConcept.findSubExpression(_ == curConceptAttr).isEmpty)
        ) {
          if(attr.value.isDefined) {
            modifiedConcept = modifiedConcept.replaceSubExpression(ConceptAttribute(Nil, attr.name), attr.value.get)
            attributesInUse = attributesInUse.map(_.replaceSubExpression(ConceptAttribute(Nil, attr.name), attr.value.get))
          }
        } else {
          if(!attributesInUse.contains(attr)) {
            attributesInUse = attr :: attributesInUse
          }
        }
      })
    })
    setAttributes(modifiedConcept, attributesInUse.reverse, context)
  }

  def findAliases(parentConcepts: List[ParentConcept], conceptName: String, context: ValidationContext): List[String] = {
    val curParentConceptAliases: List[String] = parentConcepts.flatMap(pc => {
      pc.concept match {
        case ConceptReference(curConceptName) if conceptName == curConceptName => pc.getAlias :: Nil
        case acf: AnonymousConceptDefinition => findAliases(acf.getParentConcepts(context), conceptName, context)
        case _ => Nil
      }
    })
    curParentConceptAliases
  }

  def setAttributes(concept: SensConcept, attributes: List[Attribute], context: ValidationContext): SensConcept = {
    concept match {
      case c: Concept => c.copy(attributes = attributes)
      case int: IntersectConcept => int.copy(parentConcepts = replaceAttributesInParentConcepts(int.parentConcepts, attributes, context))
      case min: MinusConcept => min.copy(parentConcepts = replaceAttributesInParentConcepts(min.parentConcepts, attributes, context))
      case un: UnionConcept => un.copy(parentConcepts = replaceAttributesInParentConcepts(un.parentConcepts, attributes, context))
      case dsc: DataSourceConcept => dsc.copy(attributes = attributes)
      case acd: AnonymousConceptDefinition => acd.copy(attributes = attributes)
      //For other concepts this operation doesn't make because they should be converted to Concept first
      case inh: InheritedConcept => inh.copy(overriddenAttributes = inh.overriddenAttributes.intersect(attributes))
      case a: SensConcept => a
    }
  }

  def replaceAttributesInParentConcepts(parentConcepts: List[ParentConcept], attributes: List[Attribute], context: ValidationContext): List[ParentConcept] = {
    parentConcepts.map(pc => {
      val pcd = pc.concept match {
        case cr: ConceptReference =>
          val alias = pc.getAlias
          val newAttributes = attributes.map(attr => Attribute(attr.name, Some(ConceptAttribute(alias :: Nil, attr.name)), attr.annotations))
          AnonymousConceptDefinition
            .builder(newAttributes, ParentConcept(cr, None, Map(), Nil) :: Nil)
            .build()
        case acd: AnonymousConceptDefinition =>
          removeAttributesFromAnonimousConcept(acd, attributes, context)
        case afd: AnonymousFunctionConceptDefinition => afd
      }
      pc.copy(concept = pcd)
    })
  }

  def removeAttributesFromAnonimousConcept(concept: AnonymousConceptDefinition, attributes: List[Attribute], context: ValidationContext): SensConceptExpression = {
    var modifiedConcept: SensConceptExpression = concept
    var attributesInUse: List[Attribute] = Nil
    concept.getAttributes(context).foreach(curAttr => {
      if(attributes.exists(attr => attr.name == curAttr.name)) {
        attributesInUse = curAttr :: attributesInUse
      } else {
        if(curAttr.value.isEmpty) {
          modifiedConcept = modifiedConcept.replaceSubExpression(ConceptAttribute(Nil, curAttr.name), curAttr.value.get)
          attributesInUse = attributesInUse.map(_.replaceSubExpression(ConceptAttribute(Nil, curAttr.name), curAttr.value.get))
        }
      }
    })
    modifiedConcept match {
      case acd: AnonymousConceptDefinition => acd.copy(attributes = attributesInUse.reverse)
      case other => other
    }
  }

}
