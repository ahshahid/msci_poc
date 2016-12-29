package com.msci.temporaldb.snappy.loaders

import java.util.Properties

import com.msci.temporaldb.snappy.common.Constants
import com.msci.temporaldb.snappy.ddl.CreateTables
import com.msci.temporaldb.snappy.loaders.common.CommonDataLoaderJob
import com.msci.temporaldb.snappy.loaders.equities.{EquityLoaderJob, FictitiousDataEquityLoader}
import org.apache.spark.sql.SnappyContext

/**
  * Created by ashahid on 12/28/16.
  */
object CreateLoadTables {

  val ddlSchemaPath =  getClass.getResource("/scripts/create_tables.sql")
  val dataGenConfig = getClass.getResource("/config/datagen_config.properties")
  val dataFilePathProps =  {
    val props = new Properties()
    props.load(getClass.getResource("/config/dataFilePaths.properties").openStream())
    props
  }

  def createAndLoad(snc: SnappyContext): Unit = {
    CreateTables.createTables(snc, ddlSchemaPath)
    CommonDataLoaderJob.loadData(snc, dataFilePathProps.getProperty(Constants.attribFileKey))
    FictitiousDataEquityLoader.loadData(snc, dataGenConfig)
    EquityLoaderJob.loadData(snc, dataFilePathProps.getProperty(Constants.instrumentsFileKey),
      dataFilePathProps.getProperty(Constants.obsFileKey))
  }

}
