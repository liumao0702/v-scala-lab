package engineer.zhangwei.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZhangWei on 2017/6/19.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"

    //Create a SparkContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("WordCountDemo")
    val sc = new SparkContext(conf)


    val textFile = sc.textFile("data/textfile/Hamlet.txt")

    val result = textFile
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    result.saveAsTextFile("target/result")
    result.count()
    result.first()
  }
}