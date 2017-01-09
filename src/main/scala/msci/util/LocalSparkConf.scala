package msci.util

import org.apache.spark.SparkConf

object LocalSparkConf {

  def newConf(addOn: (SparkConf) => SparkConf = null): SparkConf = {
    val conf = new SparkConf().
      setIfMissing("spark.master", "local[4]").
      setAppName(getClass.getName)
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "4").set("spark.ui.enabled", "true")

    if (addOn != null) {
      addOn(conf)
    }
    conf
  }
}