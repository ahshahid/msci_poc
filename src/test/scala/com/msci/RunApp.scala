package com.msci

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.mcsi.temporaldb.snappy.common.Constants
import com.mcsi.temporaldb.snappy.ddl.CreateTables
import com.mcsi.temporaldb.snappy.loaders.equities.{EquityLoaderJob, FictitiousDataEquityLoader}
import com.mcsi.temporaldb.snappy.loaders.common.CommonDataLoaderJob
import com.mcsi.temporaldb.snappy.queries.common.{AttributeCache, QueryExecutor, SnappyContextQueryExecutor}
import com.mcsi.temporaldb.snappy.queries.equities.EquityQueries
import com.msci.util.LocalSparkConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SnappyContext}

object RunApp {

  val ddlSchemaPath =  getClass.getResource("/scripts/create_tables.sql").getPath
  val baseData = getClass.getResource("/data/equities/NT_RS_SPOT_EQUITY.csv").getPath
  val obsData = getClass.getResource("/data/equities/NT_RS_OBS_EQUITY_100k.csv").getPath
  val attribTypesData  =  getClass.getResource("/data/common/attributeTypes.csv").getPath
  val dataGenConfig = getClass.getResource("/data/equities/datagen_config.properties").getPath

  def main(args: Array[String]): Unit = {
    val snc = RunApp.snc
    val queryExecutor: QueryExecutor[DataFrame] = new SnappyContextQueryExecutor(snc)
    CreateTables.createTables(snc, ddlSchemaPath)
    CommonDataLoaderJob.loadData(snc, attribTypesData)
    FictitiousDataEquityLoader.loadData(snc, dataGenConfig)
    EquityLoaderJob.loadData(snc, baseData, obsData)
    AttributeCache.initialize(queryExecutor)


    //Fire queries

    //Get the name of the equity instrument corresponding to id 9
    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()
    val data = EquityQueries.get1Value1AttribPerDayLastTimestamp[Int, DataFrame](
      equityInstrumentName, "price", queryExecutor)

   // data.foreach(println(_))

    val q1 = s"select name from ${Constants.BRF_CON_INST} as x where" +
      s" x.ID = ${Constants.TEST_INSTRUMENT_ID}"
    val equityInstrumentName1 = queryExecutor.executeQuery[String](q1, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()


    println("Total number of rows in observation table = " +
      snc.table(Constants.BTS_IR_OBS).count())

    this.testQuery1(snc, queryExecutor, equityInstrumentName1)
    this.testQuery2(snc, queryExecutor, equityInstrumentName1)
    this.testQuery3(snc, queryExecutor, equityInstrumentName1)
    this.testQuery4(snc, queryExecutor, equityInstrumentName1)
    this.testQuery5(snc, queryExecutor, equityInstrumentName1)
    this.testQuery6(snc, queryExecutor, equityInstrumentName1)
    this.testQuery7(snc, queryExecutor, equityInstrumentName1)
  }

  def testQuery1(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestamp[Int, DataFrame](
      instrumentName, "price", queryExecutor)

    val (obstime, value) = testRs.next()
    assert(!testRs.hasNext)
    println("query obs time = " + obstime)
    println("query value = " + value)
    val expectedObsTime = java.sql.Timestamp.valueOf("2016-01-02 10:00:00.0")
    val expectedValue = 127
    assert(obstime == expectedObsTime)
    assert(expectedValue == value)
  }

  def testQuery2(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampBeforeCutOff[Int, DataFrame](
      instrumentName, "price", "02:00", queryExecutor)

    val (obstime, value) = testRs.next()
    assert(!testRs.hasNext)
    println("query obs time = " + obstime)
    println("query value = " + value)
    val expectedObsTime = java.sql.Timestamp.valueOf("2016-01-02 02:00:00.0")
    val expectedValue = 200
    assert(obstime == expectedObsTime)
    assert(expectedValue == value)
  }

  def testQuery3(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {
    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampForYear[Int, DataFrame](
      equityInstrumentName, "price", 2013, queryExecutor)
    // only one attribute per day
    val mapping  = scala.collection.mutable.Map[Int, Set[Int]]()
    assert(testRs.hasNext)
    val cal = Calendar.getInstance()
    while(testRs.hasNext) {
      val (obstime, value) = testRs.next()
      cal.setTimeInMillis(obstime.getTime)
      assert(cal.get(Calendar.YEAR) == 2013)
      val month = cal.get(Calendar.MONTH)
      val day = cal.get(Calendar.DAY_OF_MONTH)
      val doesNotContain = !mapping.contains(month) || !mapping.get(month).contains(day)

      assert(doesNotContain)
      var set = mapping.getOrElse(month, Set.empty)
      set += day
      mapping.put(month, set)
    }
  }


  def testQuery4(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {

    val testRs = EquityQueries.getAllValue1AttribPerDay[Int, DataFrame](
      instrumentName, "price", queryExecutor)
    val(obs1, val1) = testRs.next()
    val(obs2, val2) = testRs.next()
    val(obs3, val3) = testRs.next()
    val(obs4, val4) = testRs.next()
    val(obs5, val5) = testRs.next()
    assert(!testRs.hasNext)

    assert(obs1 == Timestamp.valueOf("2016-01-02 02:00:00.0"))
    assert(val1 == 200)

    assert(obs2 == Timestamp.valueOf("2016-01-02 04:00:00.0"))
    assert(val2 == 124)

    assert(obs3 == Timestamp.valueOf("2016-01-02 06:00:00.0"))
    assert(val3 == 125)

    assert(obs4 == Timestamp.valueOf("2016-01-02 08:00:00.0"))
    assert(val4 == 126)

    assert(obs5 == Timestamp.valueOf("2016-01-02 10:00:00.0"))
    assert(val5 == 127)

  }

  def testQuery5(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {


    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()
    val timeTill = "2015-08-03 20:00:00"
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, DataFrame](
      equityInstrumentName, "price", timeTill , queryExecutor)
    val time = Timestamp.valueOf(timeTill)
    val mapping  = scala.collection.mutable.Map[Int, Set[Int]]()
    var currentYear = -1
    assert(testRs.hasNext)
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time.getTime)
    while(testRs.hasNext) {
      val (obstime, value) = testRs.next()
      cal.setTimeInMillis(obstime.getTime)
      assert(obstime.before(time))
      if(currentYear != cal.get(Calendar.YEAR)) {
        mapping.clear()
        currentYear = cal.get(Calendar.YEAR)
      }
      val month = cal.get(Calendar.MONTH)
      val day = cal.get(Calendar.DAY_OF_MONTH)
      val doesNotContain = !mapping.contains(month) || !mapping.get(month).contains(day)

      assert(doesNotContain)
      var set = mapping.getOrElse(month, Set.empty)
      set += day
      mapping.put(month, set)
    }

    val testRs1 = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, DataFrame](
      instrumentName, "price", "2016-01-02 03:00:00.0", queryExecutor)

    val (obstime, value) = testRs1.next()
    assert(!testRs1.hasNext)
    assert(value == 200)
    assert(obstime == Timestamp.valueOf("2016-01-02 02:00:00.0"))

    val testRs2 = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, DataFrame](
      instrumentName, "price", "2016-01-02", queryExecutor)

    val (obstime2, value2) = testRs2.next()
    assert(!testRs2.hasNext)
    assert(value2 == 127)
    assert(obstime2 == Timestamp.valueOf("2016-01-02 10:00:00.0"))
  }

  def testQuery6(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {
  val testRs1 = EquityQueries.getAllValue1AttribPerDayTillDateNoCorrection[Int, DataFrame](
      instrumentName, "price", "2016-01-02 03:00:00.0", queryExecutor)

    val (obstime, value) = testRs1.next()
    assert(!testRs1.hasNext)
    assert(value == 123)
    assert(obstime == Timestamp.valueOf("2016-01-02 02:00:00.0"))

    val testRs2 = EquityQueries.getAllValue1AttribPerDayTillDateNoCorrection[Int, DataFrame](
      instrumentName, "price", "2016-01-02", queryExecutor)

    val (obstime1, value1) = testRs2.next()
    val (obstime2, value2) = testRs2.next()
    val (obstime3, value3) = testRs2.next()
    val (obstime4, value4) = testRs2.next()
    val (obstime5, value5) = testRs2.next()
    assert(!testRs2.hasNext)
    assert(value1 == 123)
    assert(obstime1 == Timestamp.valueOf("2016-01-02 02:00:00.0"))
    assert(value2 == 124)
    assert(obstime2 == Timestamp.valueOf("2016-01-02 04:00:00.0"))
    assert(value3 == 125)
    assert(obstime3 == Timestamp.valueOf("2016-01-02 06:00:00.0"))
    assert(value4 == 126)
    assert(obstime4 == Timestamp.valueOf("2016-01-02 08:00:00.0"))
    assert(value5 == 127)
    assert(obstime5 == Timestamp.valueOf("2016-01-02 10:00:00.0"))
  }

  def testQuery7(snc: SnappyContext, queryExecutor: QueryExecutor[DataFrame],
                 instrumentName: String): Unit = {


    val testRs = EquityQueries.getAllCorrectionsValue1AttribPerDay[Int, DataFrame](
      instrumentName, "price", queryExecutor)

    val (obstime, value) = testRs.next()
    assert(!testRs.hasNext)
    println("query obs time = " + obstime)
    println("query value = " + value)
    val expectedObsTime = java.sql.Timestamp.valueOf("2016-01-02 02:00:00.0")
    val expectedValue = 123
    assert(obstime == expectedObsTime)
    assert(expectedValue == value)
  }

  protected def sc: SparkContext = {
    val ctx = SnappyContext.globalSparkContext
    if (ctx != null && !ctx.isStopped) {
      ctx
    } else {
      cachedContext = null
      new SparkContext(newSparkConf())
    }
  }


  @transient private var cachedContext: SnappyContext = _

  def getOrCreate(sc: SparkContext): SnappyContext = {
    val gnc = cachedContext
    if (gnc != null) gnc
    else synchronized {
      val gnc = cachedContext
      if (gnc != null) gnc
      else {
        cachedContext = SnappyContext(sc)
        cachedContext
      }
    }
  }

  protected def snc: SnappyContext = getOrCreate(sc)

  protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf =
    LocalSparkConf.newConf(addOn)


}
