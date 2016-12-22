package com.mcsi.temporaldb.snappy.queries.common

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SnappyContext}

import scala.reflect.ClassTag

/**
  * Created by ashahid on 12/21/16.
  */
class SnappyContextQueryExecutor(snc: SnappyContext) extends QueryExecutor[DataFrame] {

  def getTransformerQ1[K] : DataFrame => Iterator[(Timestamp, K )] = {
    df: DataFrame => {
      import snc.sparkSession.implicits._
      df.map(row => row.getTimestamp(0) -> row.getDecimal(1)).collect().iterator.map{
        case (ts, value) => (ts, value.asInstanceOf[K])
      }
    }
  }

  def getTransformerQ2 : DataFrame => Iterator[(String, Int)] = {
    df => {
      import snc.sparkSession.implicits._
      df.map(row => (row.getString(1), row.getInt(0))).collect().iterator
    }
  }
  override def executeQuery[T](queryStr: String, resultTransformer: DataFrame => Iterator[T]):
  Iterator[T]
  = {
    val df = snc.sql(queryStr)
    resultTransformer(df)
  }

}
