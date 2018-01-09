package engineer.zhangwei.storage.hbase.phoenix

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by ZhangWei on 2017/9/27.
  */
object PhoenixSparkExample {
  def main(args: Array[String]): Unit = {
//    readExample
    writeExample
  }


  def readExample = {
    val spark = SparkSession.builder
      .appName("PhoenixExample")
      .getOrCreate()

    // Load as DF using Data Source API
    val df =   spark.read
      .format("org.apache.phoenix.spark")
      .option("table","TABLE1")
      .option("zkUrl", "172.20.32.175:2181:/hbase-unsecure")
      .load()

    df
      .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
      .select(df("ID"))
      .show

    // Load as RDD
    val rdd: RDD[Map[String, AnyRef]] = spark.sparkContext.phoenixTableAsRDD(
      "TABLE1", Seq("ID", "COL1"), zkUrl = Some("172.20.32.175:2181:/hbase-unsecure")
    )
    print(rdd.count())
  }

    def writeExample = {
      val spark = SparkSession.builder
        .appName("PhoenixExample")
        .getOrCreate()
      // Load INPUT_TABLE
      val df =   spark.read
        .format("org.apache.phoenix.spark")
        .option("table","INPUT_TABLE")
        .option("zkUrl", "172.20.32.175:2181:/hbase-unsecure")
        .load()

      df.saveToPhoenix("OUTPUT_TABLE")
      // Save to OUTPUT_TABLE


    }
}
