package org.sens.core.statement
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class StatementBlock(items: List[SensStatement]) extends SensStatement {
  override def toSensString: String =
    "{\n" +
     (if (!items.isEmpty) items.map(_.toSensString).mkString("\n") + "\n" else "") +
    "}"

  override def getSubExpressions: List[SensExpression] = Nil

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensStatement] = {
    val newContext = context.addFrame
    Try(StatementBlock(
      items.map(_.validateAndRemoveVariablePlaceholders(newContext).get)
    ))
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(items, f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    items.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensStatement =
    StatementBlock(
      items.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
    )
}
