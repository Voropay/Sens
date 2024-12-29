package org.sens.converter.rel

import org.sens.parser.RecursiveDefinitionNotAllowedException

import scala.annotation.tailrec

object Utils {

  @tailrec
  def tsort[A](toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
    val (noPreds, hasPreds) = toPreds.partition {
      _._2.isEmpty
    }
    if (noPreds.isEmpty) {
      if (hasPreds.isEmpty) done else throw RecursiveDefinitionNotAllowedException(hasPreds.keys.mkString(", "))
    } else {
      val found = noPreds.keys
      tsort(hasPreds.mapValues {
        _ -- found
      }, done ++ found)
    }
  }

}
