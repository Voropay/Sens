package org.sens.core.concept

import org.sens.core.expression.{GenericParameter, NamedElementPlaceholder, SensExpression}
import org.sens.core.expression.literal.StringLiteral
import org.sens.parser.ValidationContext

import scala.util.Try

case class ExpressionIdent(expr: SensExpression) extends SensIdent {
  override def getName: String = {
    val parametersToReplace = expr.findAllSubExpressions {
      case _: GenericParameter => true
      case _: NamedElementPlaceholder => true
      case _ => false
    }
    var updatedExpr = expr
    parametersToReplace.foreach(curExpr => {
      val name = curExpr match {
        case GenericParameter(n) => n
        case NamedElementPlaceholder(n) => n
        case _ => ""
      }
      updatedExpr = updatedExpr.replaceSubExpression(curExpr, StringLiteral("$" + name))
    })
    updatedExpr.evaluate.get.stringValue
  }

  override def toSensString: String = "$" + expr.toSensString

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[ExpressionIdent] =
    Try(
      ExpressionIdent(expr.validateAndRemoveVariablePlaceholders(context).get)
    )

  override def getSubExpressions: List[SensExpression] = expr :: Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    expr.findSubExpression(f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = expr.findAllSubExpressions(f)

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensIdent =
    ExpressionIdent(expr.replaceSubExpression(replaceSubExpression, withSubExpression))
}

