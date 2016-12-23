package com.msci

import com.mcsi.temporaldb.snappy.common.Constants
import com.mcsi.temporaldb.snappy.ddl.CreateTables
import com.mcsi.temporaldb.snappy.loaders.equities.EquityLoaderJob
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
  val attribTypesData = getClass.getResource("/data/common/attributeTypes.csv").getPath
  def main(args: Array[String]): Unit = {
    val snc = RunApp.snc
    val queryExecutor: QueryExecutor[DataFrame] = new SnappyContextQueryExecutor(snc)
    CreateTables.createTables(snc, ddlSchemaPath)
    CommonDataLoaderJob.loadData(snc, attribTypesData)
    EquityLoaderJob.loadData(snc, baseData, obsData)
    AttributeCache.initialize(queryExecutor)


    //Fire queries

    //Get the name of the equity instrument corresponding to id 9
    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()
    val data = EquityQueries.getQueryString1Value1AttribPerDayLastTimestamp[Int, DataFrame](
      equityInstrumentName, "price", queryExecutor)

    data.foreach(println(_))


    val q1 = s"select name from ${Constants.BRF_CON_INST} as x where" +
      s" x.ID = ${Constants.TEST_INSTRUMENT_ID}"
    val equityInstrumentName1 = queryExecutor.executeQuery[String](q1, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()

    val testRs = EquityQueries.getQueryString1Value1AttribPerDayLastTimestamp[Int, DataFrame](
      equityInstrumentName1, "price", queryExecutor)

    val (obstime, value) = testRs.next()
    assert(!testRs.hasNext)
    println("query obs time = " + obstime)
    println("query value = " + value)
    val expectedObsTime = java.sql.Timestamp.valueOf("2016-01-02 10:00:00.0")
    val expectedValue = 127
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
