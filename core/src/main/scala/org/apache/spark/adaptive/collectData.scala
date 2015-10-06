package org.apache.spark.adaptive

import java.io.{InputStreamReader, BufferedReader}
import java.lang.management.{MemoryUsage, ManagementFactory, MemoryMXBean}

import org.apache.spark.Logging

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.sys.process._
import ExecutionContext.Implicits.global

/**
 * Created by spark on 15-9-27.
 */
private[spark] class collectData extends Runnable with Logging{

  /** collect runtime data.by yaoz*/
  private[spark] val memoryData = HashMap[Long,Double]()
  private[spark] val instanceData = HashMap[Long,(Long,Long)]()
  private[spark] val deserializeTime = HashMap[Long,Long]()
  private[spark] val serializeTime = HashMap[Long,Long]()
  private[spark] val deAndSerializationTime = HashMap[Long,Long]()

  //base metrics byte/s
  private[spark] var baseResSerTimeByByte: Double = _
  private[spark] var baseDirectResSerTimeByByte: Double = _
  private[spark] var baseDeserTimeByByte: Double = _

  //set sleep timeout
  private[spark] var sleepTimeout: Long = 3000

  override def run(): Unit = {
    while(true) {
      //===============get freeMemory==================//
      /** system JVM HEAP parameters.*/
      val memorymbean: MemoryMXBean = ManagementFactory.getMemoryMXBean
      val memoryUsage: MemoryUsage = memorymbean.getHeapMemoryUsage
      val heapMemMax = memoryUsage.getMax
      val heapMemUsage = memoryUsage.getUsed
      val heapMemFree = memoryUsage.getInit - memoryUsage.getUsed
      logInfo(s"INIT HEAP:${memoryUsage.getInit.toFloat / 1024 / 1024}MB")
      logInfo(s"MAX HEAP:${memoryUsage.getMax.toFloat / 1024 / 1024 / 1024}GB")
      logInfo(s"USE HEAP:${memoryUsage.getUsed.toFloat / 1024 / 1024}MB")
      logInfo(s"HEAP MEMORY USAGE:${heapMemUsage.toFloat / 1024 / 1024}MB")
      logInfo(s"HEAP MEMORY FREE:${heapMemFree.toFloat / 1024 / 1024}MB")
      logInfo(s"NON-HEAP MEMORY USAGE:${memorymbean.getNonHeapMemoryUsage}")

      val currentTime = System.currentTimeMillis
      memoryData(currentTime) = heapMemUsage.toDouble / heapMemMax.toDouble

      //===============get jvm heap instance number==============//
      try {
        val pid: String = ManagementFactory.getRuntimeMXBean.getName.replaceAll("(\\d+)@.*", "$1")
        //val cmd_result = Runtime.getRuntime.exec("jmap -histo " + pid)
        val cmd = Seq("jmap","-histo",pid)
        val cmd_result: StringBuilder = new StringBuilder(cmd.!!)
        cmd_result(cmd_result.length - 1) = '#'
        val text = cmd_result.toString
        val lastLine = text.substring(text.lastIndexOf("\n")+1, text.length-1)
        logInfo(s"total num of current instance is:$lastLine")
        //get instance data
        val curInstanceData = lastLine
        var totalInstanceNum = 0
        var totalInstanceBytes = 0
        var totalInstanceNumCur = curInstanceData.indexOf(" ")
        val totalInstanceBytesCur = curInstanceData.lastIndexOf(" ")
        //get all instance number in jvm
        while(curInstanceData(totalInstanceNumCur)==' ') totalInstanceNumCur += 1
        totalInstanceNum = curInstanceData.substring(totalInstanceNumCur,
          curInstanceData.indexOf(" ",totalInstanceNumCur)-1).toInt
        //get all instance volume
        totalInstanceBytes = curInstanceData.substring(totalInstanceBytesCur+1,
          curInstanceData.length-1).toInt
        //store the data
        instanceData(currentTime) = (totalInstanceNum,totalInstanceBytes)

      } catch {
        case t: Throwable =>
          logError(s"invoke jmap is failed,info is ${t.getMessage}")
      }

      //====================get I/O information====================//
      /** set collection data interval to 1ms. */
      try {
        Thread.sleep(sleepTimeout)
      }catch {
        case t: Throwable=>
          logError(s"invoking sleep occurs problem,info is ${t.getMessage}")
      }
    }
  }

  private[spark] def setSleepTimeout(timeout: Long): Unit ={
    this.sleepTimeout = timeout
  }
}

object collectData{

  //indicate whether the collecting data DeamonThread is started
  private[spark] var isStarted: Boolean = false

  //whether collector is running
  private[spark] var isRunning: Boolean = false
}