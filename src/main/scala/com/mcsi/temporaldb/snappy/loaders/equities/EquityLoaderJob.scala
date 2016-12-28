package com.mcsi.temporaldb.snappy.loaders.equities

import java.sql.Timestamp

import com.typesafe.config.Config
import com.mcsi.temporaldb.snappy.common.Constants

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{row, _}

import scala.collection.JavaConverters._


class EquityLoaderJob extends SnappySQLJob {
  def runSnappyJob(ss: SnappySession, jobConfig: Config): Any = {
     val dataFilePath1 = jobConfig.getString(Constants.dataFilePath1)
     val dataFilePath2 = jobConfig.getString(Constants.dataFilePath2)
     EquityLoaderJob.loadData(ss.sqlContext, dataFilePath1, dataFilePath2)

  }

  def isValidJob(sn: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}


object EquityLoaderJob {

  def loadData(snc: SnappyContext, dataFilePath1: String, dataFilePath2: String): Unit = {

    val df = snc.read.format("com.databricks.spark.csv").option("header", "true").load(
      dataFilePath1)

    println("\n schema obtained from file =" + df.schema)
  //  val current = new Timestamp(System.currentTimeMillis())
    //Populate the BRF_CON_INST table
    val list = scala.collection.mutable.ArrayBuffer[Row]()

    val nameSet = scala.collection.mutable.HashSet[String]()
    var id = FictitiousDataEquityLoader.endIndex
    var invalidRows = 0
    df.collect.foreach(row => {
      if(!nameSet.contains(row.getString(1))) {
        try {
          nameSet.add(row.getString(1))
          id += 1
          val idtoUse = if(row.getString(1) == Constants.TEST_INSTRUMENT_NAME) {
            Constants.TEST_INSTRUMENT_ID
          }else {
            id
          }
          list += Row(idtoUse, row.getString(1), row.getString(2), row.getString(3),
            if (!row.isNullAt(4)) row.getString(4).toInt else null,
            row.getString(5), if (row.getString(6) != null) Timestamp.valueOf(row.getString(6))
            else
              null,
            if (row.getString(7) != null) Timestamp.valueOf(row.
              getString(7))
            else null, if (row.getString(8) != null) Timestamp.valueOf(row.getString
            (8))
            else null, row.getString(9),
            row.getString(10))
        }catch {
          case e: Exception => {
            nameSet.remove(row.getString(1))
            id -= 1
            invalidRows += 1
            println( "invalid data found row = " + row)
          }
        }
      }
    })
    val baseSchema = StructType(Array(StructField("1", IntegerType), StructField("2",
      StringType), StructField("3", StringType), StructField("4", StringType), StructField("5",
      IntegerType), StructField("6", StringType), StructField("7", TimestampType),
      StructField("8", TimestampType), StructField("9", TimestampType), StructField("10",
        StringType), StructField("11", StringType)) )
    println( "Total number of invalid rows found =" + invalidRows)
    println( "Total number of valid rows found =" + id)
    if(id > 0) {

      val filteredDF = snc.createDataFrame(list.asJava, baseSchema)


      val tab1 =snc.table(Constants.BRF_CON_INST)

      val data1 = filteredDF.map((row: Row) => {
        Row(row.getInt(0), Constants.instrument_type_equity, row.
          getString(1), row.getString(2), row.getString(9), if (!row.isNullAt(8)) row
          .getTimestamp(8)
        else null,
          if (!row.isNullAt(6)) row.getTimestamp(6) else null,
          null, if (!row.isNullAt(7)) row.getTimestamp(7) else null, null)
      })(RowEncoder(tab1.schema))

      data1.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_CON_INST)


      val tab2 =snc.table(Constants.BRF_IR)
      val data2 = filteredDF.map(row => Row(row.getInt(0), row.getString(3), if (!row.isNullAt(4)) row.
        getInt(4)
      else null,
        if (!row.isNullAt(6)) row.getTimestamp(6) else null, null, if (!row.isNullAt(7)) row.
          getTimestamp(7)
        else null, null))(RowEncoder(tab2.schema))



      data2.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_IR)

      val tab3 =snc.table(Constants.BRF_IR_NODE)
      val data3 = filteredDF.map(row => Row(row.getInt(0), row.getInt(0).toInt, "mature",
        if (!row.isNullAt(6)) row.getTimestamp(6) else null, null, if (!row.isNullAt(7)) row.
          getTimestamp(7)
        else null, null))(RowEncoder(tab3.schema))


      data3.write.mode(SaveMode.Append).saveAsTable(Constants.BRF_IR_NODE)

      val tab4 =snc.table(Constants.BTS_IR_OBS)
      val df1 = snc.read.format("com.databricks.spark.csv").option("header", "true").load(
        dataFilePath2)
      val data4 = df1.map(row => Row(row.getString(0).toInt, Timestamp.valueOf(row.
        getString(1)), Constants.ATTRIBUTE_PRICE, BigDecimal((row.getString(2).trim()+"D").toDouble),
        Timestamp.valueOf(row.getString(1))))(RowEncoder(tab4.schema))


      data4.write.mode(SaveMode.Append).saveAsTable(Constants.BTS_IR_OBS)

      //Insert correction data to test instrument
      snc.sql(s"insert into ${Constants.BTS_IR_OBS} values( ${Constants.TEST_INSTRUMENT_ID}," +
        s"'2016-01-02 02:00:00.0', ${Constants.ATTRIBUTE_PRICE}, 200, '2016-01-03 02:00:00.0')" )

    }

  }

}


