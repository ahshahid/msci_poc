package com.msci.temporaldb.snappy.loaders.equities

import java.net.URL
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.msci.temporaldb.snappy.common.Constants
import com.msci.temporaldb.snappy.loaders.CreateLoadTables
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder



class FictitiousDataEquityLoader extends SnappySQLJob {
  def runSnappyJob(ss: SnappySession, jobConfig: Config): Any = {

    FictitiousDataEquityLoader.loadData(ss.sqlContext, CreateLoadTables.dataGenConfig)

  }

  def isValidJob(sn: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}


object FictitiousDataEquityLoader {

  val keyNumSecurities = "numSecurities"
  val keyNumYearsData = "numYears"
  val keyFrequencyPerDay = "frequencyPerDay"
  var endIndex: Int = 0
  val snappyInstrument = "snappy_instrument_"

  def loadData(snc: SnappyContext, configFilePath: URL): Unit = {
    val props = new Properties()
    props.load(configFilePath.openStream())
    val numSecurities = props.getProperty(keyNumSecurities, "1000").toInt
    val numYears = props.getProperty(keyNumYearsData, "20").toInt
    val freq = props.getProperty(keyFrequencyPerDay, "4").toInt
    val startIndex = 2
    endIndex = startIndex + numSecurities
    /*  ID  Integer not null primary key,
      INST_TYPE INTEGER not null,
      NAME VARCHAR(130) not null,
      DESCRIPTION VARCHAR(255) not null,
      DATASET VARCHAR(50) not null,
      START_DATE TIMESTAMP,
      VALID_RNG_START TIMESTAMP,
      VALID_RNG_END TIMESTAMP,
      TRANS_RNG_START TIMESTAMP not null,
      TRANS_RNG_END TIMESTAMP not null*/

    val cal = Calendar.getInstance()
    cal.setTimeInMillis(System.currentTimeMillis())
    cal.add(Calendar.YEAR, -numYears)
    val pastTime = cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" +
      cal.get(Calendar.DAY_OF_MONTH) + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(
      Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND)
    val pastTS = Timestamp.valueOf(pastTime)
    val startYear = cal.get(Calendar.YEAR)


    val tab1 = snc.table(Constants.BRF_CON_INST)
    val data1 = snc.range(startIndex, endIndex).map(row => {
      val id = row.getLong(0).toInt
      Row(id, Constants.instrument_type_equity, snappyInstrument + id, "snappy test instrument",
        "AAAAAAAAA", pastTS, pastTS, null, pastTS, null)

    })(RowEncoder(tab1.schema))


    data1.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_CON_INST)

    val tab2 = snc.table(Constants.BRF_IR)
    val data2 = snc.range(startIndex, endIndex).map(row => {
      Row(row.getLong(0).toInt, "Dollar", 6, pastTS, null, pastTS, null)
    })(RowEncoder(tab2.schema))


    data2.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_IR)

    val tab3 = snc.table(Constants.BRF_IR_NODE)
    val data3 = snc.range(startIndex, endIndex).map(row => {
      Row(row.getLong(0).toInt, row.getLong(0).toInt, "matured", pastTS, null, pastTS, null)
    })(RowEncoder(tab3.schema))


    data3.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_IR_NODE)


    val tab4 = snc.table(Constants.BTS_IR_OBS)
    val jump = 2
    val rndom = new scala.util.Random(123456789)
    val data4 = snc.range(startIndex, endIndex).flatMap(row => {
      val start = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(pastTime)
      val cal1 = Calendar.getInstance()
      cal1.setTime(start)
      var year = startYear
      val allRows = for (i <- 0 until numYears) yield {
        val row_row = for (j <- 0 until 360) yield {
          val rows = for (k <- 0 until freq) yield {
            val ts = cal1.get(Calendar.YEAR) + "-" + (cal1.get(Calendar.MONTH) +1) + "-" +
              cal1.get(Calendar.DAY_OF_MONTH) + " " + cal1.get(Calendar.HOUR_OF_DAY) + ":" + cal1.
              get(Calendar.MINUTE) + ":" + cal1.get(Calendar.SECOND)

            cal1.add(Calendar.HOUR_OF_DAY, jump)
            Row(row.getLong(0).toInt, Timestamp.valueOf(ts), Constants.ATTRIBUTE_PRICE, BigDecimal
            (rndom.nextDouble()),
              Timestamp.valueOf(ts))
          }
          cal1.add(Calendar.HOUR, 24 - jump * freq)
          rows
        }
        year += 1
        cal1.set(Calendar.YEAR, year)
        cal1.set(Calendar.MONTH, 0)
        cal1.set(Calendar.DAY_OF_MONTH, 1)
        cal1.set(Calendar.HOUR_OF_DAY, 2)
        cal1.set(Calendar.MINUTE, 30)
        row_row
      }
      allRows.flatten.flatten
    })(RowEncoder(tab4.schema))


    data4.write.mode(SaveMode.Overwrite).saveAsTable(Constants.BTS_IR_OBS)


  }


}


