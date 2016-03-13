package dmk.spark.streaming.model

/**
 * A summary of a window
 */
case class BaseballSummaryRecord(teamId: String, numRows: Long, key: String)