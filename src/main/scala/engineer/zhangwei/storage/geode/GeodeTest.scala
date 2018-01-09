package engineer.zhangwei.storage.geode

import engineer.zhangwei.storage.geode.util.GeodeUtil
import org.apache.geode.pdx.{JSONFormatter, PdxInstance}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json.JSONObject

/**
  * Created by ZhangWei on 2017/7/25.
  */
object GeodeTest {
  def main(args: Array[String]): Unit = {
    val partations: String = args(0) //分片数
    val spark: SparkSession = SparkSession.builder().appName("geodetest").
      config("spark.sql.inMemoryColumnarStorage.compressed", "true").
      config("spark.sql.inMemoryColumnarStorage.batchSize", "30000").
      config("spark.sql.shuffle.partitions", partations.toInt).
      config("spark.sql.autoBroadcastJoinThreshold", "20971520").
      config("spark.shuffle.file.buffer", "128k").
      config("spark.reducer.maxSizeInFlight", "128m").
      getOrCreate()
    val dataframe: DataFrame =spark.read.load("hdfs://172.20.32.211:8020/data/geodedata4000W")
    dataframe.printSchema()
    val data: Dataset[String] = dataframe.toJSON
    // data.write.text("/data/4000Wtxt")

    val startTime = System.currentTimeMillis().toDouble
    println(dataframe.count()+"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    data.foreachPartition(
      partations=> {
        val getregion=GeodeUtil.getInstance().getRegion("regionTEST")
        val rowMap = new java.util.HashMap[String,PdxInstance]()
        partations.foreach(
          row => {
            val rowJson = new JSONObject(row.toString)
            val key =rowJson.getString("ROWKEY")+"NEW22"
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
    val endTime = System.currentTimeMillis().toDouble
    println("xXxXxXxXxXx  Saved, time is :"+(endTime-startTime)/60000+"S xXxXxXxXxXxX")
  }
}
