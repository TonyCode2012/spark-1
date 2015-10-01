package org.apache.spark.adaptive

import java.io.{InputStreamReader, BufferedReader}
import java.lang.management.{MemoryUsage, ManagementFactory, MemoryMXBean}

import org.apache.spark.Logging

import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import ExecutionContext.Implicits.global

/**
 * Created by spark on 15-9-27.
 */
private[spark] class collectData extends Runnable with Logging{

  private[spark] val memoryData = HashMap[Long,Long]()
  private[spark] val instanceData = HashMap[Long,(Long,Long)]()

  /** whether collector is running.*/
  private[spark] var isRunning: Boolean = false

  /** system JVM HEAP parameters.*/
  private[spark] val memorymbean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  private[spark] val memoryUsage: MemoryUsage = memorymbean.getHeapMemoryUsage

  override def run(): Unit = {

    //===============get freeMemory==================//
    val heapMemUsage = memoryUsage.getUsed
    val heapMemFree = memoryUsage.getInit - memoryUsage.getUsed
    logInfo(s"INIT HEAP:${memoryUsage.getInit.toFloat/1024/1024}MB")
    logInfo(s"MAX HEAP:${memoryUsage.getMax.toFloat/1024/1024/1024}GB")
    logInfo(s"USE HEAP:${memoryUsage.getUsed.toFloat/1024/1024}MB")
    logInfo(s"HEAP MEMORY USAGE:${heapMemUsage.toFloat/1024/1024}MB")
    logInfo(s"HEAP MEMORY FREE:${heapMemFree.toFloat/1024/1024}MB")
    logInfo(s"NON-HEAP MEMORY USAGE:${memorymbean.getNonHeapMemoryUsage}")

    val currentTime = System.currentTimeMillis
    memoryData(currentTime) = memoryUsage.getUsed / memoryUsage.getMax
    //===============get jvm heap instance number==============//
    Future {
      val pid: String = ManagementFactory.getRuntimeMXBean.getName.replaceAll("(\\d+)@.*", "$1")
      val cmd_result = Runtime.getRuntime.exec("jmap -histo " + pid)
      cmd_result
    } onComplete {
      case Success(p:Process)=>
        val bufferReader: BufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream))
        var buf = ""
        var lastLine = ""
        while ({
          buf = bufferReader.readLine
          buf != null
        }) lastLine = buf
        logInfo(s"total num of current instance is:$lastLine")
      case Failure(t:Throwable)=>
        logError(s"invoke jmap is failed,info is ${t.getMessage}")
    }

    /** collection data interval is 1ms.*/
    Thread.sleep(1000)
  }
}

object collectData{
  /** indicate whether the collecting data DeamonThread is started*/
  var isStarted:Boolean = false
}