package org.sens.core.expression.operation.comparison

import org.sens.core.expression.operation.SensBinaryOperation
import org.sens.parser.ValidationContext

import scala.util.Try

trait SensComparison extends SensBinaryOperation {
  def name: String

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensComparison]
}
