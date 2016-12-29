package com.msci.temporaldb.snappy.queries.common

import java.sql.{DriverManager, ResultSet, Timestamp}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * Created by ashahid on 12/21/16.
  */
class JDBCQueryExecutor(jdbcUrl: String) extends QueryExecutor[ResultSet] {

  val conn = DriverManager.getConnection(jdbcUrl)
  val stmt = conn.createStatement()

  def getTransformerForObservations[K: TypeTag] : ResultSet => Iterator[(Timestamp, K )] = {
    rs: ResultSet => RSIterator[K](rs)
  }

  override val getTransformerForAttributeCache : ResultSet => Iterator[(String, Int)] =
    (rs: ResultSet) => {
      val buffer = new ArrayBuffer[(String, Int)]
      while(rs.next()) {
        buffer += (rs.getString(2) -> rs.getInt(1))
      }
      buffer.iterator
    }


  override def executeQuery[T](queryStr: String, resultTransformer: ResultSet => Iterator[T]):
  Iterator[T]
  = {
    val rs = stmt.executeQuery(queryStr)
    resultTransformer(rs)
  }

}

case class RSIterator[T: TypeTag](rs: ResultSet ) extends Iterator[(Timestamp, T)] {

  override def hasNext: Boolean = rs.next()

  override  def next: (Timestamp, T) =  {
    val ts = rs.getTimestamp(1)
    if(ts == null) {
      (null, null.asInstanceOf[T])
    }else {
      (ts, typeOf[T] match {

        case t if( t =:= typeOf[Int]) => rs.getBigDecimal(2).intValue().asInstanceOf[T]
        case t if( t =:= typeOf[Double]) => rs.getBigDecimal(2).doubleValue().asInstanceOf[T]
        case t if( t =:= typeOf[Long]) =>rs.getBigDecimal(2).longValue().asInstanceOf[T]
        case t if( t =:= typeOf[Float]) => rs.getBigDecimal(2).floatValue().asInstanceOf[T]
      }
        )
    }
  }
}
