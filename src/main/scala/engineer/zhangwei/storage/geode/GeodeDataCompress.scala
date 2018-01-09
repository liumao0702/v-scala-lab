package engineer.zhangwei.storage.geode

import engineer.zhangwei.storage.geode.util.GeodeUtil
import org.apache.geode.pdx.{JSONFormatter, PdxInstance}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json.JSONObject

/**
  * Created by ZhangWei on 2017/7/27.
  */
object GeodeDataCompress {
  def main(args: Array[String]): Unit = {
    val partations: String = args(0) //分片数·
    val spark: SparkSession = SparkSession.builder().appName("geodetest").
      config("spark.sql.inMemoryColumnarStorage.compressed", "true").
      config("spark.sql.inMemoryColumnarStorage.batchSize", "30000").
      config("spark.sql.shuffle.partitions", partations.toInt).
      config("spark.sql.autoBroadcastJoinThreshold", "20971520").
      config("spark.shuffle.file.buffer", "128k").
      config("spark.reducer.maxSizeInFlight", "128m").
      getOrCreate()

//    val dataframe_Long_12Col_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_Long_12Col_Json")
//    val dataframe_ShortString_12Col_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_ShortString_12Col_Json")
//    val dataframe_String_12Col_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_String_12Col_Json")
//    val dataframe_String_12LongCol_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_String_12LongCol_Json")
//    val dataframe_String_6Col_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_String_6Col_Json")
//    val dataframe_RandomString_12Col_Json: DataFrame =spark.read.json("hdfs://172.20.32.211:8020/data/zhangwei/1kw_RandomString_12Col_Json")


//    val data_Long_12Col_Json: Dataset[String] = dataframe_Long_12Col_Json.toJSON
//    val data_ShortString_12Col_Json: Dataset[String] = dataframe_ShortString_12Col_Json.toJSON
//    val data_String_12Col_Json: Dataset[String] = dataframe_String_12Col_Json.toJSON
//    val data_String_12LongCol_Json: Dataset[String] = dataframe_String_12LongCol_Json.toJSON
//    val data_String_6Col_Json: Dataset[String] = dataframe_String_6Col_Json.toJSON
//    val data_RandomString_12Col_Json: Dataset[String] = dataframe_RandomString_12Col_Json.toJSON
    // data.write.text("/data/4000Wtxt")
//    saveGeodeData(data_Long_12Col_Json, "C_Long_12Col_Json", "Row1")
//    saveGeodeData(data_ShortString_12Col_Json, "C_ShortString_12Col_Json", "Row1")
//    saveGeodeData(data_String_12Col_Json, "C_String_12Col_Json", "Row1")
//    saveGeodeData(data_String_12LongCol_Json, "C_String_12LongCol_Json", "LongLongLongRow1")
//    saveGeodeData(data_String_6Col_Json, "C_String_6Col_Json", "Row1")
//    saveGeodeData(data_RandomString_12Col_Json, "C_RandomString_12Col_Json", "Row1")
//    saveGeodeData(data_Long_12Col_Json, "UC_Long_12Col_Json", "Row1")
//    saveGeodeData(data_ShortString_12Col_Json, "UC_ShortString_12Col_Json", "Row1")
//    saveGeodeData(data_String_12Col_Json, "UC_String_12Col_Json", "Row1")
//    saveGeodeData(data_String_12LongCol_Json, "UC_String_12LongCol_Json", "LongLongLongRow1")
//    saveGeodeData(data_String_6Col_Json, "UC_String_6Col_Json", "Row1")
//    saveGeodeData(data_RandomString_12Col_Json, "UC_RandomString_12Col_Json", "Row1")

  }

  def saveGeodeData(data:Dataset[String], reginName:String, keyName:String):Unit = {
    data.foreachPartition(
      partations=> {
        val getregion=GeodeUtil.getInstance().getRegion(reginName)
        val rowMap = new java.util.HashMap[String,PdxInstance]()
        partations.foreach(
          row => {
            val rowJson = new JSONObject(row.toString)
            val key =rowJson.getString(keyName)
            // val key = rowJson.getString("MP_ID") + rowJson.getString("DAY_DATE") + rowJson.getString("S_ID").replace(" ", "").replace(".", "").trim
            if (rowMap.size != 20000)
              rowMap.put(key.toString, JSONFormatter.fromJSON(row.toString))
            else {
              getregion.putAll(rowMap)
              rowMap.clear()
              rowMap.put(key.toString, JSONFormatter.fromJSON(row.toString))
            }
          }
        )
        if(!rowMap.isEmpty) {
          getregion.putAll(rowMap)
          rowMap.clear()
        }
      }
    )
  }


}
