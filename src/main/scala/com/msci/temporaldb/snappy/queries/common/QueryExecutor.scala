package com.msci.temporaldb.snappy.queries.common

import java.sql.Timestamp

import scala.reflect.runtime.universe._

/**
  * Created by ashahid on 12/21/16.
  */
trait QueryExecutor[R] {

  def executeQuery[T](queryStr: String, resultTransformer: R => Iterator[T]): Iterator[T]

  def getTransformerForObservations[K: TypeTag] : R => Iterator[(Timestamp, K)]

  def getTransformerForAttributeCache : R => Iterator[(String, Int)]

}
