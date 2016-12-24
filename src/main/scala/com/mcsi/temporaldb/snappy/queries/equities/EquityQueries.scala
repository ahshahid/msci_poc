package com.mcsi.temporaldb.snappy.queries.equities

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mcsi.temporaldb.snappy.common.Constants
import com.mcsi.temporaldb.snappy.queries.common.{AttributeCache, QueryExecutor}
import org.apache.spark.sql.Row

import scala.reflect.runtime.universe._

/**
  * Created by ashahid on 12/21/16.
  */
object EquityQueries {


  val cutOffTimeFormatters = Seq(new SimpleDateFormat("HH"), new SimpleDateFormat("HH a"),
     new SimpleDateFormat("HH:mm a"), new SimpleDateFormat("HH:mm"),
    new SimpleDateFormat("HH:mm:ss"), new SimpleDateFormat("HH:mm:ss a"))

  val q5_6JustDateFormatter =  new SimpleDateFormat("yyyy-mm-dd")

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
                 (hour(obs_date) * 3600 + minute(obs_date) *60 + second(obs_date)) <= '%3$$s' and
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID
                  and obs.val_type =  %2$$s  order by observation_time window win as
                  ( partition by date_format(obs_date,'yyyy.MM.dd') order by obs_date ,
                  obs.TRANS_RNG_START  rows between unbounded preceding and unbounded following)"""

  val query3 = s"""select distinct last_value(obs_date) over win as observation_time,
                 last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 year(obs_date) = %3$$s  and  instr.Name = '%1$$s'
                  and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                  order by observation_time window win as
                  ( partition by date_format(obs_date,'yyyy.MM.dd') order by obs_date ,
                  obs.TRANS_RNG_START  rows between unbounded preceding and unbounded following)"""

  val query4 = s"""select distinct obs_date  as observation_time,
                  last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                 order by observation_time window win as
                 (partition by obs_date  order by  obs.TRANS_RNG_START
                  rows between unbounded preceding and unbounded following)"""

  val query5 = s"""select distinct last_value(obs_date) over win as observation_time,
                 last_value(val) over win  from  ${Constants.BTS_IR_OBS} as obs,
                 ${Constants.BRF_CON_INST} as instr where
                 obs_date <= '%3$$s' and  instr.Name = '%1$$s' and instr.ID = obs.NODE_ID
                  and obs.val_type =  %2$$s  order by observation_time window win as
                  ( partition by date_format(obs_date,'yyyy.MM.dd') order by obs_date ,
                  obs.TRANS_RNG_START  rows between unbounded preceding and unbounded following)"""

  val query6 = s"""select distinct obs_date  as observation_time,
                  last_value(val) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                 and obs_date <= '%3$$s' and obs.TRANS_RNG_START <= '%3$$s'
                 order by observation_time window win as
                 (partition by obs_date  order by  obs.TRANS_RNG_START
                  rows between unbounded preceding and unbounded following)"""


  val query7 = s"""select distinct lag(obs_date, 1) over win as observation_time,
                  lag(val, 1) over win  from
                 ${Constants.BTS_IR_OBS} as obs,  ${Constants.BRF_CON_INST} as instr where
                 instr.Name = '%1$$s' and instr.ID = obs.NODE_ID  and obs.val_type =  %2$$s
                 order by observation_time window win as
                 (partition by obs_date  order by  obs.TRANS_RNG_START
                  rows between  1 preceding and 1 preceding)"""



  def get1Value1AttribPerDayLastTimestamp[T: TypeTag, R](instrumentName: String,
      attributeName: String, queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)] =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query1.format(instrumentName, valType)
    println("equity query1 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }


  /**
    *
    * @param instrumentName equity name
    * @param attributeName  attribute name of the instrument like price , yield , volume etc
    * @param cutoffTime Time in the string format of the form HH:mm:ss ( hour minute second) or
    *                   (HH:mm) or  HH:mm a for am pm marker or HH or HH a  for am pm marker
    * @param queryExecutor QueryExecutor
    * @tparam T Numerical data type in which the result is needed ( can be Int, Float, Double ..etc)
    * @tparam R Depending upon the type of executor can be DataFrame or java.sql.ResultSet
    * @return Iterator[(Timestamp, T)] An iterator of  tuple containing the observation time & the
    *         observation attribute
    */
  def get1Value1AttribPerDayLastTimestampBeforeCutOff[T: TypeTag, R](instrumentName: String,
    attributeName: String, cutoffTime: String, queryExecutor: QueryExecutor[R]):
  Iterator[(Timestamp,T)] =  {
    var totalSecs = -1
    val iter = cutOffTimeFormatters.iterator
    while(totalSecs == -1 && iter.hasNext ) {
      val df = iter.next
      try {
        val date = df.parse(cutoffTime)
        val cal = Calendar.getInstance()
        cal.setTime(date)
        totalSecs = cal.get(Calendar.HOUR_OF_DAY) * 3600 + cal.get(Calendar.MINUTE) * 60 +
        cal.get(Calendar.SECOND)
      }catch {
        case e: Exception =>
      }
    }
    if(totalSecs == -1) {
      throw new IllegalArgumentException(" cut off time provided as" + cutoffTime + " is " +
        "unparsable")
    }

    val valType = AttributeCache.getValType(attributeName)
    val query = query2.format(instrumentName, valType, totalSecs)
    println("equity query2 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }


  def get1Value1AttribPerDayLastTimestampForYear[T: TypeTag, R](instrumentName: String,
    attributeName: String, year: Int, queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)]
  =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query3.format(instrumentName, valType, year)
    println("equity query3 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }

  def getAllValue1AttribPerDay[T: TypeTag, R](instrumentName: String,
  attributeName: String, queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)]
  =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query4.format(instrumentName, valType)
    println("equity query4 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }

  def get1Value1AttribPerDayLastTimestampTillDate[T: TypeTag, R](instrumentName: String,
         attributeName: String, tillDate: String, queryExecutor: QueryExecutor[R]):
  Iterator[(Timestamp, T)]  =  {
    val ts = {
      try {
        Timestamp.valueOf(tillDate).getTime
      }catch {
        case e: Exception => q5_6JustDateFormatter.parse(tillDate).getTime
      }
    }

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(ts)
    val hr = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val sec = cal.get(Calendar.SECOND)
    val modDate = if(hr == 0 && min == 1 && sec == 0) {
      //add time till end of day
      cal.add(Calendar.HOUR_OF_DAY, 23)
      cal.add(Calendar.MINUTE, 58)
      cal.add(Calendar.SECOND, 59)
      cal.get(Calendar.YEAR) +"-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar
        .DAY_OF_MONTH) + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) +
      ":" + cal.get(Calendar.SECOND)
    }else {
      tillDate
    }
    val valType = AttributeCache.getValType(attributeName)
    val query = query5.format(instrumentName, valType, modDate)
    println("equity query5 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }

  def getAllValue1AttribPerDayTillDateNoCorrection[T: TypeTag, R](instrumentName: String,
       attributeName: String, tillDate: String, queryExecutor: QueryExecutor[R]):
  Iterator[(Timestamp, T)]  =  {
    val ts = {
      try {
        Timestamp.valueOf(tillDate).getTime
      }catch {
        case e: Exception => q5_6JustDateFormatter.parse(tillDate).getTime
      }
    }
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(ts)
    val hr = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val sec = cal.get(Calendar.SECOND)
    val modDate = if(hr == 0 && min == 1 && sec == 0) {
      //add time till end of day
      cal.add(Calendar.HOUR_OF_DAY, 23)
      cal.add(Calendar.MINUTE, 58)
      cal.add(Calendar.SECOND, 59)
      cal.get(Calendar.YEAR) +"-" + (cal.get(Calendar.MONTH) + 1) +"-" + cal.get(Calendar
        .DAY_OF_MONTH) + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) +
        ":" + cal.get(Calendar.SECOND)
    }else {
      tillDate
    }
    val valType = AttributeCache.getValType(attributeName)
    val query = query6.format(instrumentName, valType, modDate)
    println("equity query6 = " + query)
    queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor.getTransformerForObservations[T])
  }

  def getAllCorrectionsValue1AttribPerDay[T: TypeTag, R](instrumentName: String,
     attributeName: String, queryExecutor: QueryExecutor[R]): Iterator[(Timestamp, T)]
  =  {
    val valType = AttributeCache.getValType(attributeName)
    val query = query7.format(instrumentName, valType)
    println("equity query7 = " + query)
   val iter =  queryExecutor.executeQuery[(Timestamp,T)](query, queryExecutor
      .getTransformerForObservations[T])
    //filter out null which may be the first element
    if(iter.hasNext) {
      val firstElem = iter.next()
      if(firstElem._1 == null) {
        iter
      }else {
        Iterator.single(firstElem) ++(iter)
      }
    }else {
      iter
    }

  }

}
