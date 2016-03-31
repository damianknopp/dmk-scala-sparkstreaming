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

    val percentageTx = .02f
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
    model.generateAssociationRules(minConfidence)
      .collect
      .foreach { rule =>
        println(
          rule.antecedent.mkString("[", ",", "]")
            + " => " + rule.consequent.mkString("[", ",", "]")
            + ", " + rule.confidence)
      }
    
    /**
[36], 2936                                                                      
[36,38], 2790
[36,38,39], 1945
[36,39], 2037
[39], 50675
[110], 2794
[110,38], 2725
[48], 42135
[48,39], 29142
[38], 15596
[38,48], 7944
[38,48,39], 6102
[38,39], 10345
[310], 2594
[310,39], 1852
[101], 2237
[32], 15167
[32,38], 2833
[32,38,39], 1840
[32,48], 8034
[32,48,39], 5402
[32,39], 8455
[475], 2167
[41], 14945
[41,38], 3897
[41,38,48], 2374
[41,38,48,39], 1991
[41,38,39], 3051
[41,48], 9018
[41,48,39], 7366
[41,32], 3196
[41,32,48], 2063
[41,32,39], 2359
[41,39], 11414
[65], 4472
[65,48], 2529
[65,48,39], 1797
[65,39], 2787
[271], 2094
[413], 1880
[89], 3837
[89,48], 2798
[89,48,39], 2125
[89,39], 2749
[438], 1863
[225], 3257
[225,39], 2351
[170], 3099
[170,38], 3031
[170,38,39], 2019
[170,39], 2059
[1327], 1786
[147], 1779
[237], 3032
[237,39], 1929


===association rules
[32,48] => [39], 0.6723923325865073
[36,39] => [38], 0.9548355424644085
[65,39] => [48], 0.6447793326157158
[170,38] => [39], 0.6661167931375783
[41,32] => [48], 0.64549436795995
[41,32] => [39], 0.7381101376720901
[41] => [48], 0.603412512546002
[41] => [39], 0.7637336901973905
[38,48] => [39], 0.7681268882175226
[32,38] => [39], 0.6494881750794211
[310] => [39], 0.7139552814186585
[36] => [38], 0.9502724795640327
[36] => [39], 0.6938010899182562
[41,39] => [48], 0.6453478184685474
[65,48] => [39], 0.7105575326215896
[170] => [38], 0.9780574378831881
[170] => [39], 0.6644078735075831
[38] => [39], 0.6633111054116441
[41,38,39] => [48], 0.6525729269092101
[89,39] => [48], 0.7730083666787922
[110] => [38], 0.9753042233357194
[225] => [39], 0.7218299048203869
[89] => [48], 0.729215532968465
[89] => [39], 0.7164451394318478
[170,39] => [38], 0.9805730937348227
[41,38,48] => [39], 0.8386689132266217
[89,48] => [39], 0.7594710507505361
[65] => [39], 0.623211091234347
[32,39] => [48], 0.6389118864577173
[237] => [39], 0.6362137203166227
[36,38] => [39], 0.6971326164874552
[41,48] => [39], 0.8168108227988468
[41,38] => [48], 0.609186553759302
[41,38] => [39], 0.7829099307159353
[48] => [39], 0.6916340334638661
     */
  }

}