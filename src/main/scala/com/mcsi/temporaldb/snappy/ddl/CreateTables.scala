package com.mcsi.temporaldb.snappy.ddl

import java.io.{File, FileReader}
import java.sql.Timestamp
import java.util.Scanner

import com.mcsi.temporaldb.snappy.common.Constants
import com.typesafe.config.Config
import org.apache.spark.sql._


object CreateTables {
  def createTables(snc: SnappyContext, sqlScript: String): Unit = {
    val file = new File(sqlScript)
    val scanner = new Scanner(file)
    scanner.useDelimiter(";")
    while(scanner.hasNext) {
      val sql = scanner.next()
      println("executing sql = " +sql)
      snc.sql(sql)
    }
  }

}
