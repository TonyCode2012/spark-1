package org.apache.spark

import java.io.FileWriter
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

/**
 * Created by spark on 15-11-3.
 */
/* SimpleApp.scala */

object test_1 extends Logging{
  def main(args: Array[String]) {
    // Should be some file on your system
    val applicationName = "SimpleCount"
    val dataVolume = 1
    val resultPath = "/home/spark/spark/experiment/test/ASS.txt"
    val logFile = s"hdfs://192.168.1.100:9000/spark/data/action${dataVolume.toString}GB"
    //System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //System.setProperty("spark.kryo.registrator", "com.sigmoidanalytics.MyRegistrator")
    val conf = new SparkConf().setAppName(applicationName)
    val sc = new SparkContext(conf)
    //var wordcount=sc.textFile(logFile)
    //println(wordcount.count)
    //val beforeJobStart = System.
    val runTime = new ArrayBuffer[Double]()
    for(i <- 0 to 1){
      val jobStartTime = System.currentTimeMillis()
      val logDataToFilter = sc.textFile(logFile)
        .map(l=>l.split("\t"))
        .map(l=>(l(2),1)).cache()
        .reduceByKey(_+_,2).cache()
        .filter(x=>x._2>100).cache()
      logDataToFilter.collect()
        .sortWith(_._2>_._2)
        .take(10)
        .map(x=>(x._1,x._2))
        .foreach(println)
      val jobFinishTime = System.currentTimeMillis()
      val jobCostTime = (jobFinishTime.toDouble - jobStartTime.toDouble) / 1000
      runTime += jobCostTime
      logInfo(s"job cost time is: ${jobCostTime}s")
    }
    val f: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: java.util.Date = new java.util.Date()
    val now: String = f.format(date)
    val fileWriter: java.io.FileWriter = new java.io.FileWriter(resultPath,true)
    val content: String =
      s"/+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++/\n" +
      s"/+                                                             +/\n" +
      s"/+             Experiment Time:${now}             +/\n" +
      s"/+             data format is (volume,costTime)                +/\n" +
      s"/+                                                             +/\n" +
      s"/+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++/\n" +
      "                                                                  \n" +
      s"--- Application:${applicationName} benchMark:DataVolume(${dataVolume}GB) ---\n" +
      "------------------------------------------------------------------\n" +
      s"result as follows:\n" +
      {
        var tmpStr: StringBuilder = new StringBuilder()
        for(i <- 0 until runTime.length)
          tmpStr ++= s"(${dataVolume},${runTime(i)})\n"
        tmpStr.toString
      } +
      s"average time is:\n" +
      s"(${dataVolume},${runTime.sum / runTime.length})\n" +
      "\n" +
      "\n" +
      "\n"
    fileWriter.write(content)
    fileWriter.close()
    //val totalTime = runTime.sum
    //logInfo(s"job num is:10, cost time is: ${totalTime}s, Average time is:${totalTime / 10}s")
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
