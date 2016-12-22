package com.mcsi.temporaldb.snappy.queries.equities

import java.sql.Timestamp

import com.mcsi.temporaldb.snappy.common.Constants
import com.mcsi.temporaldb.snappy.queries.common.{AttributeCache, QueryExecutor}
import org.apache.spark.sql.Row

/**
  * Created by ashahid on 12/21/16.
  */
object EquityQueries {


  val query1 = s"""select obs_date, last_value(val) over (partition by date_format(
                 obs_date, 'dd.MM.yyyy')
      order by obs.TRANS_RNG_START ) from ${Constants.BTS_IR_OBS} as obs, ${Constants.BRF_CON_INST}
      as instr where instr.Name = '%1$$s' and instr.ID = obs.NODE_ID and obs.val_type =
      %2$$s"""



  def getQueryString1Value1AttribPerDayLastTimestamp[T, R](instrumentName: String,
    attributeName: String , queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)] =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query1.format(instrumentName, valType)
    println("equity query1 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerQ1[T])
  }

}
