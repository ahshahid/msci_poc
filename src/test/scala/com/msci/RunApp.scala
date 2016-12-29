package com.msci

import java.sql.Timestamp
import java.util.Calendar

import com.msci.temporaldb.snappy.queries.common.{AttributeCache, QueryExecutor}
import com.msci.temporaldb.snappy.queries.equities.EquityQueries

object RunApp {


  def run[T](queryExecutor: QueryExecutor[T], instrumentNameID9: String,
             testInstrumentName: String): Unit = {


    AttributeCache.initialize(queryExecutor)


    //Fire queries


    val data = EquityQueries.get1Value1AttribPerDayLastTimestamp[Int, T](
      instrumentNameID9, "price", queryExecutor)

    data.foreach(println(_))

    this.testQuery1(queryExecutor, testInstrumentName)
    this.testQuery2(queryExecutor, testInstrumentName)
    this.testQuery3(queryExecutor, testInstrumentName, instrumentNameID9)
    this.testQuery4(queryExecutor, testInstrumentName)
    this.testQuery5(queryExecutor, testInstrumentName, instrumentNameID9)
    this.testQuery6(queryExecutor, testInstrumentName)
    this.testQuery7(queryExecutor, testInstrumentName)
  }

  def testQuery1[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String): Unit = {
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestamp[Int, T](
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

  def testQuery2[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String): Unit = {
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampBeforeCutOff[Int, T](
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

  def testQuery3[T](queryExecutor: QueryExecutor[T],
                 instrumentName: String, instrumentNameID9: String): Unit = {

    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampForYear[Int, T](
      instrumentNameID9, "price", 2013, queryExecutor)
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


  def testQuery4[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String): Unit = {

    val testRs = EquityQueries.getAllValue1AttribPerDay[Int, T](
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

  def testQuery5[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String, instrumentNameID9: String): Unit = {



    val timeTill = "2015-08-03 20:00:00"
    val testRs = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, T](
      instrumentNameID9, "price", timeTill , queryExecutor)
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

    val testRs1 = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, T](
      instrumentName, "price", "2016-01-02 03:00:00.0", queryExecutor)

    val (obstime, value) = testRs1.next()
    assert(!testRs1.hasNext)
    assert(value == 200)
    assert(obstime == Timestamp.valueOf("2016-01-02 02:00:00.0"))

    val testRs2 = EquityQueries.get1Value1AttribPerDayLastTimestampTillDate[Int, T](
      instrumentName, "price", "2016-01-02", queryExecutor)

    val (obstime2, value2) = testRs2.next()
    assert(!testRs2.hasNext)
    assert(value2 == 127)
    assert(obstime2 == Timestamp.valueOf("2016-01-02 10:00:00.0"))
  }

  def testQuery6[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String): Unit = {
  val testRs1 = EquityQueries.getAllValue1AttribPerDayTillDateNoCorrection[Int, T](
      instrumentName, "price", "2016-01-02 03:00:00.0", queryExecutor)

    val (obstime, value) = testRs1.next()
    assert(!testRs1.hasNext)
    assert(value == 123)
    assert(obstime == Timestamp.valueOf("2016-01-02 02:00:00.0"))

    val testRs2 = EquityQueries.getAllValue1AttribPerDayTillDateNoCorrection[Int, T](
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

  def testQuery7[T]( queryExecutor: QueryExecutor[T],
                 instrumentName: String): Unit = {


    val testRs = EquityQueries.getAllCorrectionsValue1AttribPerDay[Int, T](
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



}
