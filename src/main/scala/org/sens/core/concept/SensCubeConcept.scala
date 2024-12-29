package org.sens.core.concept

import org.sens.core.expression.ConceptAttribute
import org.sens.parser.ValidationContext

trait SensCubeConcept extends SensConcept {
  def getMetrics(context: ValidationContext): List[Attribute]
  def getDimensions(context: ValidationContext): List[Attribute]
  def toCubeConcept(context: ValidationContext): CubeConcept
  def toConcept(context: ValidationContext): Concept

  override def isTransparent: Boolean = true
}
