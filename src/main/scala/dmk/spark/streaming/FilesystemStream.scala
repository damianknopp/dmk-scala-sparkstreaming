package dmk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import dmk.spark.streaming.util.LogLevelUtil
import dmk.spark.streaming.util.SparkConfUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

/**
 * http://localhost:4040/SQL/
 */
object FilesystemStream {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: FilesystemStream <indir> <outdir>")
      System.exit(1)
    }

    LogLevelUtil.reduceLogLevels()

    val sparkConf = new SparkConf().setAppName("FilesystemStream")
    SparkConfUtil.setParallelism(sparkConf)
    SparkConfUtil.setKryo(sparkConf)
    SparkConfUtil.setMemoryFraction(sparkConf)
    SparkConfUtil.setBackPressure(sparkConf)
    SparkConfUtil.setWAL(sparkConf)

    val windowDuration = Milliseconds(2000 * 2)
    val slideDuration = windowDuration
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.checkpoint("checkpoint")
    val indir = args(0)
    val outdir = args(1)
    val stream: DStream[String] = ssc.textFileStream(indir)

    stream.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val rowsDf = rdd.map(line => {
        val arr = line.split(",")
        Record(arr(0), arr(1), arr(2), arr(3))
      }).toDF()
      rowsDf.registerTempTable("baseball")
      val rowCountsDf =
        sqlContext.sql("select teamId, count(*) as numRows from baseball group by teamId")

      val summary = s"$outdir"
      //        val schema = genSchema
      // summary across all time, schema read error on first read w/ no data
      //      val dfReader = sqlContext.read
      //      val df1 = dfReader.parquet(summary)
      //      val summaryDf = rowsDf.join(df1)
      //      summaryDf.map(r => (r(0).toString, r(1).asInstanceOf[Long]) ).reduceByKey(_ + _).toDF
      println(s"========= $time =========")
      rowCountsDf.show()
      val rows = rowCountsDf.count
      if (rows > 0) {
        // this batch had data so write it out to its partition
        val outf = s"$outdir/key=$time"
        println(s"writing $rows to $outf")
        rowCountsDf.write.parquet(outf)
        //        summaryDf.write.parquet(summary)
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

  def genSchema(): StructType = {
    val struct =
      StructType(
        StructField("id", StringType, true) ::
          StructField("yearId", StringType, false) ::
          StructField("teamId", StringType, false) ::
          StructField("rank", StringType, true) :: Nil)
    struct
  }
}

case class Record(id: String, yearId: String, teamId: String, rank: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}