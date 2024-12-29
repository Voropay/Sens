package org.sens.converter.rel

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystem}
import org.apache.calcite.schema.{Function, FunctionParameter}
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}

import java.util

class PushedDownFunctionImpl(params: List[String]) extends Function {
  override def getParameters: util.List[FunctionParameter] = {
    val javaList = new util.ArrayList[FunctionParameter](params.size)
    var i = 0
    for(curParam <- params) {
      javaList.add(new DefaultFunctionParameterImpl(i, curParam))
      i += 1
    }
    javaList
  }
}

class DefaultFunctionParameterImpl(ordinal: Int, name: String) extends FunctionParameter {

  override def getOrdinal: Int = ordinal

  override def getName: String = name

  override def getType(typeFactory: RelDataTypeFactory): RelDataType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ANY)

  override def isOptional: Boolean = false
}
