package org.sens.converter.rel

import com.google.common.collect.{ImmutableList, ImmutableSortedMap, ImmutableSortedSet}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.schema.{Function, Schema, SchemaVersion, Table}

class VirtualSchema extends CalciteSchema (
  null,
  new AbstractSchema(),
  "vs",
  null,
  null,
  null,
  null,
  null,
  null,
  null,
  null
) {

  override def getImplicitSubSchema(schemaName: String, caseSensitive: Boolean): CalciteSchema = null

  override def getImplicitTable(tableName: String, caseSensitive: Boolean): CalciteSchema.TableEntry = null

  override def getImplicitType(name: String, caseSensitive: Boolean): CalciteSchema.TypeEntry = null

  override def getImplicitTableBasedOnNullaryFunction(tableName: String, caseSensitive: Boolean): CalciteSchema.TableEntry = null

  override def addImplicitSubSchemaToBuilder(builder: ImmutableSortedMap.Builder[String, CalciteSchema]): Unit = {}

  override def addImplicitTableToBuilder(builder: ImmutableSortedSet.Builder[String]): Unit = {}

  override def addImplicitFunctionsToBuilder(builder: ImmutableList.Builder[Function], name: String, caseSensitive: Boolean): Unit = {}

  override def addImplicitFuncNamesToBuilder(builder: ImmutableSortedSet.Builder[String]): Unit = {}

  override def addImplicitTypeNamesToBuilder(builder: ImmutableSortedSet.Builder[String]): Unit = {}

  override def addImplicitTablesBasedOnNullaryFunctionsToBuilder(builder: ImmutableSortedMap.Builder[String, Table]): Unit = {}

  override def snapshot(parent: CalciteSchema, version: SchemaVersion): CalciteSchema = this.snapshot(parent, version)

  override def isCacheEnabled: Boolean = false

  override def setCache(cache: Boolean): Unit = {}

  override def add(name: String, schema: Schema): CalciteSchema = null
}
