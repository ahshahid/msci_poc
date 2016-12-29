package com.msci

import com.msci.temporaldb.snappy.common.Constants
import com.msci.temporaldb.snappy.queries.common.JDBCQueryExecutor


/**
  * Created by ashahid on 12/28/16.
  */
object RunAppWithJDBC {
  val jdbcUrl = s"jdbc:snappydata://localhost:1527/"
  def main(args: Array[String]): Unit = {

    val queryExecutor = new JDBCQueryExecutor(jdbcUrl)


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

    RunApp.run(queryExecutor, equityInstrumentName, equityInstrumentName1)

  }

}
