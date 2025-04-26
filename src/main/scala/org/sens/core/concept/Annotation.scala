package org.sens.core.concept

import org.sens.core.SensElement
import org.sens.core.expression.SensExpression
import org.sens.core.expression.literal.{ListLiteral, SensLiteral, StringLiteral}
import org.sens.parser.{ValidationContext, WrongTypeException}

import scala.util.Try

case class Annotation(name: String,
                      attributes: Map[String, SensExpression]) extends SensElement {
  override def toSensString: String =
    "@" + name +
    (if (!attributes.isEmpty) " (" + formatAttributes(attributes) + ")" else "")

  def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[Annotation] = {
    Try(Annotation(
      name,
      attributes.map(item => (item._1, item._2.validateAndRemoveVariablePlaceholders(context).get))
    ))
  }

  override def getSubExpressions: List[SensExpression] = attributes.values.toList

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    collectFirstSubExpression(attributes.values, f)

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    attributes.values.flatMap(_.findAllSubExpressions(f)).toList
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): Annotation = {
    val newAttrs = attributes.map(item => {
      val newAttr = item._2.replaceSubExpression(replaceSubExpression, withSubExpression) match {
        case newAttr: SensLiteral => newAttr
        case other => throw WrongTypeException(this.getClass.getTypeName, other.getClass.getTypeName, "SensLiteral")
      }
      (item._1, newAttr)
    })
    Annotation(name, newAttrs)
  }

  def formatAttributes(attributes: Map[String, SensExpression]): String = {
    attributes.map(kv => kv._1 + " = " + kv._2.toSensString).mkString(", ")
  }
}

object Annotation {

  val MATERIALIZED: String = "Materialized"
  val TYPE: String = "type"
  val EPHEMERAL_TYPE = "Ephemeral"
  val TABLE_TYPE = "Table"
  val VIEW_TYPE = "View"
  val MATERIALIZED_VIEW_TYPE = "MaterializedView"
  val INCREMENTAL_TYPE = "Incremental"
  val TARGET_NAME: String = "TargetName"
  val UNIQUE_KEY: String = "uniqueKey"
  val OPERATOR: String = "operator"
  val DEFAULT: String = "Default"
  val STRATEGY_TYPE: String = "strategy"
  val DELETE_INSERT_STRATEGY = "InsertDelete"
  val INSERT_OVERRIDE_MERGE_STRATEGY = "InsertOverrideMerge"
  val MERGE_STRATEGY = "Merge"
  val APPEND_STRATEGY = "Append"


  val OPTIONAL = Annotation("Optional", Map())
  val UNIQUE = Annotation("Unique", Map())
  val INPUT = Annotation("Input", Map())
  val MATERIALIZED_EPHEMERAL = Annotation(MATERIALIZED, Map(TYPE -> StringLiteral(EPHEMERAL_TYPE)))
  val MATERIALIZED_TABLE = Annotation(MATERIALIZED, Map(TYPE -> StringLiteral(TABLE_TYPE)))
  val MATERIALIZED_VIEW = Annotation(MATERIALIZED, Map(TYPE -> StringLiteral(VIEW_TYPE)))
  val MATERIALIZED_MATERIALIZED_VIEW = Annotation(MATERIALIZED, Map(TYPE -> StringLiteral(MATERIALIZED_VIEW_TYPE)))
  def MATERIALIZED_INCREMENTAL(uniqueKey: List[String]) = Annotation(
    MATERIALIZED,
    Map(TYPE -> StringLiteral(INCREMENTAL_TYPE), "uniqueKey" -> ListLiteral(uniqueKey.map(StringLiteral))))

  val PRIVATE = Annotation("Private", Map())
}
