/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */

package dmk.spark.streaming.kafka

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import dmk.spark.streaming.util.LogLevelUtil

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    LogLevelUtil.reduceLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    setKafkaRate(sparkConf)
    setBackPressure(sparkConf)
    setKryo(sparkConf)
    setMemoryFraction(sparkConf)
    setWAL(sparkConf)
    val windowDuration = Milliseconds(2000)
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    /**
     * to handle multiple receivers, union them, they will be replicated as one RDD
     * val numStreams = 4
     * val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createStream(...) }
     * val unifiedStream = streamingContext.union(kafkaStreams)
     * unifiedStream.print()
     */

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), windowDuration, 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
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
    // deprecated message as of spark 1.6
    // by default spark give 60 percent memory to RDD cache
    conf.set("spark.storage.memoryFraction", "0.4")
    conf
  }
  
  def setWAL(conf: SparkConf): SparkConf = {
    // setting write ahead logs
    //https://databricks.com/blog/2015/01/15/improved-driver-fault-tolerance-and-zero-data-loss-in-spark-streaming.html
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    conf
  }
  
  def setKafkaRate(conf: SparkConf): SparkConf = {
    val maxMsgPerSec = 100000 * 9
    conf.set("spark.streaming.kafak.maxRatePerPartition", maxMsgPerSec.toString)
    conf
  }
  
//  def createKafkaParams(brokers: String) : Map[String, String] = {
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
//    kafkaParams.put("auto.offset.reset", "smallest")
//    kafkaParams
//  }
}
// scalastyle:on println