package engineer.zhangwei.storage.hbase

import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._
/**
  * Created by ZhangWei on 2017/10/9.
  */
object HbaseWriteFkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Load Data")
      .getOrCreate()
    val properties: Properties = new Properties
    val in: InputStream = getClass.getClassLoader.getResourceAsStream("storage-test.properties")
    properties.load(new BufferedInputStream(in))
    val inputCbxxParquetPath =  properties.getProperty("input.cbxx.parquet.path")
    val outputParuetPhoenixTable =  properties.getProperty("output.parquet.phoenix.table")
    val outputHbaseUrl = properties.getProperty("output.hbase.url")
    val repartitionNum = properties.getProperty("repartition.num")
    val coalesceNum = properties.getProperty("coalesce.num")
    val df = spark.read.parquet(inputCbxxParquetPath)


    //Phoenix 参数调优
    val conf:Configuration = new Configuration
    conf.setLong("phoenix.upsert.batch.size", 10000L)


    if (repartitionNum.toInt > 0){
      df.repartition(repartitionNum.toInt).saveToPhoenix(outputParuetPhoenixTable, zkUrl = Some(outputHbaseUrl))
    }else if(coalesceNum.toInt >0){
      df.coalesce(coalesceNum.toInt).saveToPhoenix(outputParuetPhoenixTable, zkUrl = Some(outputHbaseUrl))
    }else{
      df.saveToPhoenix(outputParuetPhoenixTable, zkUrl = Some(outputHbaseUrl))
    }
    df.repartition(30).saveToPhoenix("TEST_XJ", conf, zkUrl = Some("train01.haiyi.com:2181:/hbase-unsecure"))

    //    print(df.rdd.getNumPartitions)

  }
}
