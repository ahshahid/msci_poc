package com.msci.temporaldb.snappy.loaders

import com.typesafe.config.Config
import org.apache.spark.sql._

/**
  * Created by ashahid on 12/28/16.
  */
class MasterJob extends SnappySQLJob {
  def runSnappyJob(ss: SnappySession, jobConfig: Config): Any = {
    CreateLoadTables.createAndLoad(ss.sqlContext)
  }

  def isValidJob(sn: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }


}


