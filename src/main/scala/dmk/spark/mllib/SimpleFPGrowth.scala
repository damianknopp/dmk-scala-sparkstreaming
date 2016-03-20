package dmk.spark.mllib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import dmk.spark.streaming.util.SparkConfUtil
import org.apache.spark.SparkContext
import dmk.spark.streaming.util.LogLevelUtil

/**
 * http://localhost:4040/
 * 
 * Code mostly taken from Apache Spark examples
 * 
 * Data from Belgium retail market dataset 
 *  http://www.recsyswiki.com/wiki/Grocery_shopping_datasets
 *  http://fimi.ua.ac.be/data/retail.pdf
 */

object SimpleFPGrowth {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SimpleFPGrowth <filename>")
      System.exit(1)
    }

    LogLevelUtil.reduceLogLevels()
    
    val sparkConf = new SparkConf().setAppName("SimpleFPGrowth")
    SparkConfUtil.setParallelism(sparkConf)
//    SparkConfUtil.setKryo(sparkConf)
    
    val sparkContext = new SparkContext(sparkConf)
    val data = sparkContext.textFile(args(0))

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val percentageTx = .05f
    val numPartitions = 10
    // with numPartitions workers
    //   count things seen at least percentageTx
    val fpg = new FPGrowth().setMinSupport(percentageTx).setNumPartitions(numPartitions)
    val model = fpg.run(transactions)

    println("\n\n===freqItemsets")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.6
    println("\n\n===association rules")
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    
    /**
     *
[41] => [48], 0.603412512546002
[41] => [39], 0.7637336901973905
[38,48] => [39], 0.7681268882175226
[32,39] => [48], 0.6389118864577173
[32,48] => [39], 0.6723923325865073
[48] => [39], 0.6916340334638661
[38] => [39], 0.6633111054116441
[41,48] => [39], 0.8168108227988468
[41,39] => [48], 0.6453478184685474
     */
  }

}