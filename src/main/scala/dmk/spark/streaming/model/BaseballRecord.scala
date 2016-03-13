package dmk.spark.streaming.model

/**
 * A row in our spreadsheet
 */
case class BaseballRecord(id: String, yearId: String, teamId: String, rank: String)