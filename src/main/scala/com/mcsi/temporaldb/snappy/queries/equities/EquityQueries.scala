package com.mcsi.temporaldb.snappy.queries.equities

import java.sql.Timestamp

import com.mcsi.temporaldb.snappy.common.Constants
import com.mcsi.temporaldb.snappy.queries.common.{AttributeCache, QueryExecutor}
import org.apache.spark.sql.Row
import scala.reflect.runtime.universe._

/**
  * Created by ashahid on 12/21/16.
  */
object EquityQueries {


  val query1 = s"""select distinct last_value(obs_date) over win as observation_time,
                  last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                 order by observation_time window win as
                 (partition by date_format(obs_date,'yyyy.MM.dd')
                 order by obs_date , obs.TRANS_RNG_START
                  rows between unbounded preceding and unbounded following)"""

  val query2 = s"""select distinct last_value(obs_date) over win as observation_time,
                 last_value(val) over win  from  ${Constants.BTS_IR_OBS} as obs,
                 ${Constants.BRF_CON_INST} as instr where
                 obs_date <= '%3$$s' and  instr.Name = '%1$$s' and instr.ID = obs.NODE_ID
                  and obs.val_type =  %2$$s  order by obs_date window win as
                  ( partition by date_format(obs_date,'yyyy.MM.dd') order by observation_time ,
                  obs.TRANS_RNG_START  rows between unbounded preceding and unbounded following)"""

  val query3 = s"""select distinct last_value(obs_date) over win as observation_time,
                 last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 year(obs_date) <= %3$$s and year(obs_date) > (%3$$s -1) and and  instr.Name =
                 '%1$$s' and instr.ID = obs.NODE_ID
                  and obs.val_type =  %2$$s  order by observation_time window win as
                  ( partition by date_format(obs_date,'yyyy.MM.dd') order by obs_date ,
                  obs.TRANS_RNG_START  rows between unbounded preceding and unbounded following)"""

  val query4 = s"""select distinct obs_date  as observation_time,
                  last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                 order by observation_time window win as
                 (partition by obs_date  order by  obs.TRANS_RNG_START
                  rows between unbounded preceding and unbounded following)"""



  def getQueryString1Value1AttribPerDayLastTimestamp[T: TypeTag, R](instrumentName: String,
    attributeName: String , queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)] =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query1.format(instrumentName, valType)
    println("equity query1 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerQ1[T])
  }


  def getQueryString1Value1AttribPerDayLastTimestampBeforeCutOff[T: TypeTag, R](instrumentName:
                                                                              String,
      attributeName: String , cutoffTime: String,  queryExecutor: QueryExecutor[R]): Iterator[
    (Timestamp,
    T)]
    =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query2.format(instrumentName, valType, cutoffTime)
    println("equity query2 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerQ1[T])
  }


  def getQueryString1Value1AttribPerDayLastTimestampForYear[T: TypeTag, R](instrumentName: String,
      attributeName: String , year: Int,  queryExecutor: QueryExecutor[R]): Iterator[
    (Timestamp,
      T)]
  =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query3.format(instrumentName, valType, year)
    println("equity query3 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerQ1[T])
  }

  def getQueryString1Value1AttribPerDayLastTimestampForYear[T: TypeTag, R](instrumentName: String,
      attributeName: String , queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)]
  =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query4.format(instrumentName, valType)
    println("equity query4 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerQ1[T])
  }

}
