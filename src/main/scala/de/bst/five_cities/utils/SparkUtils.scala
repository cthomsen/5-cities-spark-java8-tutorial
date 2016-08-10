package de.bst.five_cities.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  private var sparkContext: Option[SparkContext] = None

  def getSparkContext: SparkContext = {
    sparkContext getOrElse {
      val sparkConf: SparkConf = new SparkConf
      sparkConf.setAppName("Hallo Spark!")
      sparkConf.setMaster("local[*]")
      sparkContext = Some(new SparkContext(sparkConf))
      sparkContext.get
    }
  }

  def close {
    sparkContext foreach (_.stop)
    sparkContext = None
  }

  def debugPrint[T](something: T): T = {
    println(s">> $something")
    something
  }
}
