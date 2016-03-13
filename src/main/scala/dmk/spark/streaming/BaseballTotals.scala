package dmk.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import dmk.spark.streaming.util.LogLevelUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import dmk.spark.streaming.util.SparkConfUtil
import dmk.spark.streaming.model.BaseballSummaryRecord

/**
 *
 */
object BaseballTotals {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: BaseballTotals <indir>")
      System.exit(1)
    }

    val indir = args(0)
    init(indir)
  }

  def init(indir: String): Unit = {
    LogLevelUtil.reduceLogLevels()

    val sparkConf = new SparkConf().setAppName("BaseballTotals")
    SparkConfUtil.setParallelism(sparkConf)
    SparkConfUtil.setKryo(sparkConf)
    SparkConfUtil.setMemoryFraction(sparkConf)

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val dfReader = sqlContext.read
    val summariesDf = dfReader.parquet(indir)

    // Using Datasets
    val bbDs = summariesDf.as[BaseballSummaryRecord]
//    val projectDs = bbDs.map( r => (r.teamId, r.numRows))
    // number of distinct teamId across all parquet keys
//   bbDs.select(expr("teamId").as[String]).distinct.count 
    
    // Using Dataframes
    // sum numRows of like teamIds, across all parquet keys
    //     sort and display results
    summariesDf.map(r => (r(0).toString, r(1).asInstanceOf[Long]))
                      .reduceByKey(_ + _).sortBy(_._2).toDF.show(100)
  }
}