package dmk.spark.streaming.util

import org.apache.spark.SparkConf

/**
 * @author dmknopp
 */
object SparkConfUtil {

  def setParallelism(conf: SparkConf): SparkConf = {
    // if local mode, use num cpus x 2
    val p = Runtime.getRuntime.availableProcessors() * 2
    conf.set("spark.default.parallelism", Integer.toString(p))
    // org.apache.spark.HashPartitioner uses above as max partitions
    conf
  }

  def setBackPressure(conf: SparkConf): SparkConf = {
    // spark 1.4 and before, spark 1.5 is dynamic if spark.streaming.backpressure.enabled=true
    // backpressure is good with kafka, but will fill up tcp connection cache
    // backpressure uses  proportional–integral–derivative pid
    // https://en.wikipedia.org/wiki/PID_controller
    //spark.streaming.receiver.maxRate max number of messages received per second
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf
  }

  def setKryo(conf: SparkConf): SparkConf = {
    // use kryo serializer instead of default java serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    SparkConf.registerKryoClasses()
    conf
  }

  def setMemoryFraction(conf: SparkConf): SparkConf = {
    // deprecated message as of spark 1.6, memory is unified 1.6+
    // spark <= 1.5 by default spark give 60 percent memory to RDD cache
    conf.set("spark.storage.memoryFraction", "0.4")
    conf
  }

  def setWAL(conf: SparkConf): SparkConf = {
    // setting write ahead logs
    //https://databricks.com/blog/2015/01/15/improved-driver-fault-tolerance-and-zero-data-loss-in-spark-streaming.html
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    conf
  }

}