package com.msci.temporaldb.snappy.ddl

import java.net.URL
import java.util.Scanner

import org.apache.spark.sql._


object CreateTables {
  def createTables(snc: SnappyContext, sqlScript: URL): Unit = {
    val scanner = new Scanner(sqlScript.openStream())
    scanner.useDelimiter(";")
    while(scanner.hasNext) {
      val sql = scanner.next()
      println("executing sql = " +sql)
      snc.sql(sql)
    }
  }

}
