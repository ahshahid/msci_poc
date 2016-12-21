package com.msci

import com.mcsi.temporaldb.snappy.ddl.CreateTables
import com.mcsi.temporaldb.snappy.loaders.equities.EquityLoaderJob
import com.msci.util.LocalSparkConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SnappyContext

object RunApp {

  val ddlSchemaPath =  getClass.getResource("/scripts/create_tables.sql").getPath
  val baseData = getClass.getResource("/data/equities/NT_RS_SPOT_EQUITY.csv").getPath
  val obsData = getClass.getResource("/data/equities/NT_RS_OBS_EQUITY_100k.csv").getPath
  def main(args: Array[String]): Unit = {
    val snc = RunApp.snc
    CreateTables.createTables(snc, ddlSchemaPath)
    EquityLoaderJob.loadData(snc, baseData, obsData)
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
