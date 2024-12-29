package org.sens.core.datasource

import org.sens.core.SensElement
import org.sens.core.datasource.FileFormats.FileFormat
import org.sens.core.expression.SensExpression

case class FileDataSource(path: String, format: FileFormat) extends DataSource {
  override def name: String = path

  override def toSensString: String = format + " file \"" + path + "\""

  override def getSubExpressions: List[SensExpression] = Nil

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] = None

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = Nil

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): DataSource = this
}

object FileFormats extends Enumeration {
  type FileFormat = Value
  val CSV, XML, JSON, PARQUET, AVRO, ORC = Value
}
