package org.apache.spark

/**
 * Created by spark on 15-11-3.
 */
/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import org.apache.hadoop.mapred._
//import org.apache.hadoop.io._

object test_1 {
  def main(args: Array[String]) {
    // Should be some file on your system
    val logFile = "hdfs://192.168.1.100:9000/spark/data/actions"
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //System.setProperty("spark.kryo.registrator", "com.sigmoidanalytics.MyRegistrator")
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    //var wordcount=sc.textFile(logFile)
    //println(wordcount.count)
    //val beforeJobStart = System.
    val logData = sc.textFile(logFile)
      .map(l=>l.split("\t"))
      .map(l=>(l(2),1))
      .reduceByKey(_+_,2)
      .filter(x=>x._2>100)
      .collect.sortWith(_._2>_._2)
      .take(10).map(x=>(x._1,x._2))
      .foreach(println)
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
