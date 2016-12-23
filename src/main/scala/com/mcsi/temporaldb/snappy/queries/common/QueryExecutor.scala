package com.mcsi.temporaldb.snappy.queries.common

import java.sql.Timestamp
import scala.reflect.runtime.universe._
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
  * Created by ashahid on 12/21/16.
  */
trait QueryExecutor[R] {

  def executeQuery[T](queryStr: String, resultTransformer: R => Iterator[T]): Iterator[T]

  def getTransformerQ1[K: TypeTag] : R => Iterator[(Timestamp, K)]

  def getTransformerQ2 : R => Iterator[(String, Int)]

}
