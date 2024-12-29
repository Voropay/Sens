package org.sens.core.statement

import org.sens.core.expression.SensExpression

trait SensFunctionDefinition extends SensStatement {
  def getName: String
  def validateArguments(currentArguments: List[SensExpression]): Boolean
}
