package msci

import com.msci.temporaldb.snappy.common.Constants
import com.msci.temporaldb.snappy.loaders.CreateLoadTables
import com.msci.temporaldb.snappy.loaders.equities.FictitiousDataEquityLoader
import com.msci.temporaldb.snappy.queries.common.{AttributeCache, SnappyContextQueryExecutor}

import msci.util.LocalSparkConf
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ashahid on 12/28/16.
  */
object RunAppWithSnappyContext {

  def main(args: Array[String]): Unit = {
   val snc = getOrCreate(sc)
   CreateLoadTables.createAndLoad(snc)
   val queryExecutor = new SnappyContextQueryExecutor(snc)
    AttributeCache.initialize(queryExecutor)
    println("Total number of rows in observation table = " +
      snc.table(Constants.BTS_IR_OBS).count())
    //Get the name of the equity instrument corresponding to id 9
    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()

    val q1 = s"select name from ${Constants.BRF_CON_INST} as x where" +
      s" x.ID = ${Constants.TEST_INSTRUMENT_ID}"
    val equityInstrumentName1 = queryExecutor.executeQuery[String](q1, df => {
      df.collect().map(row => row.getString(0)).toIterator
    }).next()

  // RunApp.run(queryExecutor, equityInstrumentName, equityInstrumentName1)
    RunApp.run(queryExecutor, "9", Constants.TEST_INSTRUMENT_ID.toString)
    RunApp.testPerf("4", "price", 10, queryExecutor )
   //RunApp.testPerf(FictitiousDataEquityLoader.snappyInstrument + 4, "price", 10, queryExecutor )

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


