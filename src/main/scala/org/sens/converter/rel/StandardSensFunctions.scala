package org.sens.converter.rel

import org.apache.calcite.rex.RexSubQuery
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable

object StandardSensFunctions {
  val standardSensFunctions: Map[String, SqlOperator] = Map(
    //String functions
    "concat" -> SqlStdOperatorTable.CONCAT,
    "like" -> SqlStdOperatorTable.LIKE,
    "length" -> SqlStdOperatorTable.CHAR_LENGTH,
    "upper" -> SqlStdOperatorTable.UPPER,
    "lower" -> SqlStdOperatorTable.LOWER,
    "substring" -> SqlStdOperatorTable.SUBSTRING,
    "replace" -> SqlStdOperatorTable.REPLACE,
    //Casting
    "cast" -> SqlStdOperatorTable.CAST,
    //Math functions
    "mod" -> SqlStdOperatorTable.MOD,
    "round" -> SqlStdOperatorTable.ROUND,
    "ceil" -> SqlStdOperatorTable.CEIL,
    "floor" -> SqlStdOperatorTable.FLOOR,
    "trunc" -> SqlStdOperatorTable.TRUNCATE,
    "abs" -> SqlStdOperatorTable.ABS,
    "sqrt" -> SqlStdOperatorTable.SQRT,
    //Other
    "coalesce" -> SqlStdOperatorTable.COALESCE,
    //DateTime functions
    "current_time" -> SqlStdOperatorTable.CURRENT_TIME,
    "current_date" -> SqlStdOperatorTable.CURRENT_DATE,
    "current_timestamp" -> SqlStdOperatorTable.CURRENT_TIMESTAMP,

    "sum" -> SqlStdOperatorTable.SUM,
    "count" -> SqlStdOperatorTable.COUNT,
    "max" -> SqlStdOperatorTable.MAX,
    "min" -> SqlStdOperatorTable.MIN,
    "avg" -> SqlStdOperatorTable.AVG
  )

  val standardSensAggregateFunctions: List[String] = "count" :: "max" :: "min" :: "avg" :: "sum" :: Nil
}
