package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.concept.Order.Direction
import org.sens.core.expression.SensExpression
import org.sens.parser.ValidationContext

import scala.util.Try

case class Order(attribute: SensExpression,
                 direction: Direction) extends SensElement {
  override def toSensString: String = attribute.toSensString + " " + direction

  override def getSubExpressions: List[SensExpression] = attribute :: Nil

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[Order] = {
    Try(Order(
      attribute.validateAndRemoveVariablePlaceholders(context).get,
      direction
    ))
  }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attribute.findAllSubExpressions(f)
  }

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    attribute.findSubExpression(f)

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): Order =
    Order(
      attribute.replaceSubExpression(replaceSubExpression, withSubExpression),
      direction)
}

object Order extends Enumeration {
  type Direction = Value
  val ASC, DESC = Value
}
