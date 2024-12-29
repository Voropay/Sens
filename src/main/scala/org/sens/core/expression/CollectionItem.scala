package org.sens.core.expression

import org.sens.parser.{ValidationContext, ValidationException}

import scala.util.Try

case class CollectionItem(collectionName: SensExpression, keysChain: List[SensExpression]) extends SensExpression {
  override def toSensString: String = collectionName.toSensString + keysChain.map("[" + _.toSensString + "]").mkString

  override def validateAndRemoveVariablePlaceholders(context: ValidationContext): Try[SensExpression] = {
    Try(CollectionItem(
      collectionName.validateAndRemoveVariablePlaceholders(context).get,
      keysChain.map(_.validateAndRemoveVariablePlaceholders(context).get)
    ))
  }

  def toCollectionItemChain: CollectionItem = {
    if(keysChain.isEmpty) {
      throw new ValidationException("Collection Item keysChain is empty")
    }
    if(keysChain.size == 1) {
      this
    } else {
      var baseCollection = CollectionItem(collectionName, keysChain.head :: Nil)
      for(curKey <- keysChain.tail) {
        baseCollection = CollectionItem(baseCollection, curKey :: Nil)
      }
      baseCollection
    }
  }

  override def getSubExpressions: List[SensExpression] = collectionName :: keysChain

  override def findSubExpression(f: SensExpression => Boolean): Option[SensExpression] =
    if (f(this)) {
      Some(this)
    } else {
      collectionName.findSubExpression(f)
        .orElse(collectFirstSubExpression(keysChain, f))
    }

  override def findAllSubExpressions(f: SensExpression => Boolean): List[SensExpression] = {
    val thisExpr = if (f(this)) List(this) else Nil
    thisExpr ::: collectionName.findAllSubExpressions(f) ::: keysChain.flatMap(_.findAllSubExpressions(f))
  }

  override def replaceSubExpression(replaceSubExpression: SensExpression, withSubExpression: SensExpression): SensExpression =
    if (this == replaceSubExpression) {
      withSubExpression
    } else {
      CollectionItem(
        collectionName.replaceSubExpression(replaceSubExpression, withSubExpression),
        keysChain.map(_.replaceSubExpression(replaceSubExpression, withSubExpression))
      )
    }
}
