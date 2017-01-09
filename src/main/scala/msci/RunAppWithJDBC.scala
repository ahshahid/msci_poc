package msci

import com.msci.temporaldb.snappy.common.Constants
import com.msci.temporaldb.snappy.loaders.equities.FictitiousDataEquityLoader
import com.msci.temporaldb.snappy.queries.common.{AttributeCache, JDBCQueryExecutor}
import com.msci.temporaldb.snappy.queries.equities.EquityQueries


/**
  * Created by ashahid on 12/28/16.
  */
object RunAppWithJDBC {
  val jdbcUrl = s"jdbc:snappydata://localhost:1527/"
  def main(args: Array[String]): Unit = {

    val queryExecutor = new JDBCQueryExecutor(jdbcUrl)
    AttributeCache.initialize(queryExecutor)
    println("initialized attribute cache")
    //Get the name of the equity instrument corresponding to id 9
    val q = s"select name from ${Constants.BRF_CON_INST} as x where x.ID = 9"
    val equityInstrumentName = queryExecutor.executeQuery[String](q, rs => {
      rs.next()
      Iterator(rs.getString(1))
    }).next()

    val q1 = s"select name from ${Constants.BRF_CON_INST} as x where" +
      s" x.ID = ${Constants.TEST_INSTRUMENT_ID}"
    val equityInstrumentName1 = queryExecutor.executeQuery[String](q1, rs => {
      rs.next()
      Iterator(rs.getString(1))
    }).next()

    val q2 = s"select count(*) from ${Constants.BTS_IR_OBS} "
    val numRows = queryExecutor.executeQuery[Long](q2, rs => {
      rs.next()
      Iterator(rs.getLong(1))
    }).next()

    println("Total number of row in observation table =" + numRows)


    //RunApp.run(queryExecutor, equityInstrumentName, equityInstrumentName1)
    //RunApp.run(queryExecutor, "9", s"${Constants.TEST_INSTRUMENT_ID}")
    RunApp.testPerf( "4", "price", 100, queryExecutor )
   // RunApp.testPerf(FictitiousDataEquityLoader.snappyInstrument + 4, "price", 80, queryExecutor )



  }

}
