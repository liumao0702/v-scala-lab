package engineer.zhangwei.storage.geode

import java.util.UUID

import engineer.zhangwei.storage.geode.util.RowkeyUtil
import org.apache.spark.sql.{SaveMode, SparkSession}
import sun.security.util.KeyUtil

import scala.util.Random

case class RecordString(Row1: String, Row2: String,Row3: String, Row4: String,Row5: String, Row6: String,Row7: String, Row8: String,Row9: String, Row10: String,Row11: String, Row12: String)

case class RecordLong(Row1: Long, Row2: Long,Row3: Long, Row4: Long,Row5: Long, Row6: Long,Row7: Long, Row8: Long,Row9: Long, Row10: Long,Row11: Long, Row12: Long)
//case class RecordShortString(Row1: String, Row2: String,Row3: String, Row4: String,Row5: String, Row6: String)
case class RecordLongRowString(LongLongLongRow1: String, LongLongLongRow2: String,LongLongLongRow3: String, LongLongLongRow4: String,LongLongLongRow5: String, LongLongLongRow6: String,LongLongLongRow7: String, LongLongLongRow8: String,LongLongLongRow9: String, LongLongLongRow10: String,LongLongLongRow11: String, LongLongLongRow12: String)
/**
  * Created by ZhangWei on 2017/7/26.
  */
object GeodeDataCreate {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("GeodeDataGenerator")
      .config("spark.sql.parquet.compression.codec","uncompressed")
      .getOrCreate()
    val stringVal = RowkeyUtil.getRanString()
//    val longVal = RowkeyUtil.getRanLong()
    for (i <- 1 to 100){
      spark.createDataFrame((1 to 100000).map(i => RecordString(RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString()))).write.mode(SaveMode.Append).json("hdfs://172.20.32.211/data/zhangwei/1kw_RandomString_12Col_Json")
    }
    spark.createDataFrame((1 to 1000000).map(i => RecordString(RowkeyUtil.getRanString(),stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal))).write.mode(SaveMode.Append).json("hdfs://172.20.32.211/data/zhangwei/1kwStringJson")
    spark.createDataFrame((1 to 1000000).map(i => RecordString(RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString(),RowkeyUtil.getRanString()))).write.mode(SaveMode.Append).json("hdfs://172.20.32.211/data/zhangwei/1kw_RandomString_12Col_Json")
    spark.createDataFrame((1 to 1000000).map(i => RecordLongRowString(RowkeyUtil.getRanString(),stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal,stringVal))).write.mode(SaveMode.Append).json("hdfs://172.20.32.211/data/zhangwei/1kwStringJson")
   // spark.createDataFrame((1 to 1000000).map(i => RecordLong(RowkeyUtil.getRanLong(),longVal,longVal,longVal,longVal,longVal,longVal,longVal,longVal,longVal,longVal,longVal))).write.mode(SaveMode.Append).json("hdfs://172.20.32.211/data/zhangwei/1kwLongJson")

  }

}
