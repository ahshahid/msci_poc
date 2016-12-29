package com.msci.temporaldb.snappy.queries.common

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SnappyContext}

import scala.reflect.runtime.universe._

/**
  * Created by ashahid on 12/21/16.
  */
class SnappyContextQueryExecutor(snc: SnappyContext) extends QueryExecutor[DataFrame] {

  def getTransformerForObservations[K: TypeTag] : DataFrame => Iterator[(Timestamp, K )] = {
    df: DataFrame => {
      import snc.sparkSession.implicits._
      df.map(row => row.getTimestamp(0) -> row.getDecimal(1)).collect().iterator.map{
        case (ts, value) => (ts, typeOf[K] match {
          case t if(value == null) => null.asInstanceOf[K]
          case t if( t =:= typeOf[Int]) => value.intValue().asInstanceOf[K]
          case t if( t =:= typeOf[Double]) => value.doubleValue().asInstanceOf[K]
          case t if( t =:= typeOf[Long]) => value.longValue().asInstanceOf[K]
          case t if( t =:= typeOf[Float]) => value.floatValue().asInstanceOf[K]
        } )
      }
    }
  }

  override val getTransformerForAttributeCache : DataFrame => Iterator[(String, Int)] = {
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
