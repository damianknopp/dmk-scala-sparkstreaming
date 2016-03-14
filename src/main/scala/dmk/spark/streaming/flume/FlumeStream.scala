package dmk.spark.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.rdd.RDD
import dmk.spark.streaming.util.LogLevelUtil
import dmk.spark.streaming.util.SparkConfUtil
import dmk.spark.streaming.util.SQLContextSingleton

/**
 * Spark console
 * http://localhost:4040/
 * Flume metrics
 * http://localhost:9080/metrics
 */
object FlumeStream {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: FlumeStream <host:port,[host:port,]>")
      System.exit(1)
    }

    LogLevelUtil.reduceLogLevels()

    val sparkConf = new SparkConf().setAppName("FlumeStream")
    SparkConfUtil.setParallelism(sparkConf)
    SparkConfUtil.setKryo(sparkConf)
    SparkConfUtil.setMemoryFraction(sparkConf)
    SparkConfUtil.setBackPressure(sparkConf)
    SparkConfUtil.setWAL(sparkConf)

    val windowDuration = Milliseconds(2000 * 2)
    val slideDuration = windowDuration
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.checkpoint("checkpoint")

    val streams = args.map(host => {
      val arr = host.split(":")
      val flumeStream = FlumeUtils.createStream(ssc, arr(0), Integer.parseInt(arr(1)))
      flumeStream
    })

    val unionStream = ssc.union(streams)

    unionStream.foreachRDD((rdd: RDD[SparkFlumeEvent], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      rdd.foreachPartition { x => 
        println(x)
        x.foreach { e => 
          println(e)
          val msg = new String(e.event.getBody.array())
          println(msg)  
        }
      }
      
      println(s"---- $time ----")
     
    })

    ssc.start()
    ssc.awaitTermination()
  }

}