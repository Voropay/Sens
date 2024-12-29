package org.sens.core.datasource

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression

trait DataSource extends SensElement {
  def name: String
  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): DataSource
}
