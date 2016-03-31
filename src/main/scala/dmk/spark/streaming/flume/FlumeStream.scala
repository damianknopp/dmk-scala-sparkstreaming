package dmk.spark.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import dmk.spark.streaming.util.LogLevelUtil
import dmk.spark.streaming.util.SQLContextSingleton
import dmk.spark.streaming.util.SparkConfUtil
import dmk.spark.streaming.model.HttpSummaryRecord

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

    val windowDuration = Milliseconds(1000 * 45 * 1)
    //val slideDuration = Milliseconds(1000 * 45)
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
      mapAsDf(rdd, time, sqlContext)
      println(s"---- $time ----")
    })

    ssc.start()
    ssc.awaitTermination()
  }
  
  def mapAsDf(rdd: RDD[SparkFlumeEvent], time: Time, sqlContext: SQLContext): Unit = {
      val outdir = "http-summary"
      import sqlContext.implicits._
      val rowsDf = rdd.map { e => 
        val msg = new String(e.event.getBody.array()) 
//        println(msg)
        val arr = msg.split(" ")
        HttpSummaryRecord(arr(0), Integer.parseInt(arr(arr.length - 2)))
      }.toDF()
      rowsDf.registerTempTable("logs")
      val rowCountsDf =
        sqlContext.sql("select referer, count(*) as numRequests from logs group by referer")

      val summary = s"$outdir"
      rowCountsDf.show(50)
      val rows = rowCountsDf.count
      if (rows > 0) {
        // this batch had data so write it out to its partition
        val outf = s"$outdir/key=$time"
        println(s"writing $rows to $outf")
        rowCountsDf.write.parquet(outf)
        //        summaryDf.write.parquet(summary)
      }
  }
  
  def mapPerPartition(rdd: RDD[SparkFlumeEvent], time: Time, sqlContext: SQLContext): Unit = {
    rdd.foreachPartition { x => 
        println(x)
        x.foreach { e => 
          val msg = new String(e.event.getBody.array())
          println(msg)  
        }
      }
  }

}