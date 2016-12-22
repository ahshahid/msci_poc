package com.mcsi.temporaldb.snappy.loaders.common
import java.sql.Timestamp

import com.typesafe.config.Config
import com.mcsi.temporaldb.snappy.common.Constants
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{row, _}

import scala.collection.JavaConverters._


class CommonDataLoaderJob extends SnappySQLJob {
  def runSnappyJob(ss: SnappySession, jobConfig: Config): Any = {
    val dataFilePath1 = jobConfig.getString(Constants.dataFilePathAttributeTypes)

    CommonDataLoaderJob.loadData(ss.sqlContext, dataFilePath1)

  }

  def isValidJob(sn: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}


object CommonDataLoaderJob {

  def loadData(snc: SnappyContext, dataFilePath1: String): Unit = {

    val df0 = snc.read.format("com.databricks.spark.csv").option("header", "true").load(
      dataFilePath1)
    val list0 = df0.collect().map(row => Row(row.getString(0).toInt, row.getString(1))).toList
    val tab0 =snc.table(Constants.BRF_VAL_TYPE)
    val map0 = snc.createDataFrame(list0.asJava, tab0.schema)
    map0.write.mode(SaveMode.Overwrite).saveAsTable(Constants.BRF_VAL_TYPE)
    }

}


