package com.msci.temporaldb.snappy.queries.common

import com.msci.temporaldb.snappy.common.Constants

/**
  * Created by ashahid on 12/21/16.
  */
object AttributeCache {

  @volatile var mapping : Map[String, Int] = _

  def initialize[K](queryExecutor: QueryExecutor[K]): Unit = {
    mapping = queryExecutor.executeQuery[(String, Int)](
      s"select * from ${Constants.BRF_VAL_TYPE}", queryExecutor.getTransformerForAttributeCache ).toMap

  }


  def getValType(desc: String): Int = mapping.getOrElse(desc, -1)

}
