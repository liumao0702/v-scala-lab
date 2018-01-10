package engineer.zhangwei.storage.kudu
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

/**
  * Created by ZhangWei on 2017/9/8.
  */
object CollectDemo {
  def main(args: Array[String]): Unit = {
    //Args Issue
    var masterUrl = "local[1]"

    val spark = SparkSession.builder
      .master(masterUrl)
      .appName("KuduTest")
      .getOrCreate()

    val df = spark.read.options(Map(
      "kudu.master" -> "172.20.32.162:7051",
      "kudu.table" -> "metrics")).kudu
    df.createOrReplaceTempView("metrics")

    // Print the first five values
    spark.sql("select * from metrics limit 5").show()
    spark.sql("select count(*) from metrics").show()
    // Calculate the average value of every host/metric pair
    spark.sql("select host, metric, avg(value) from metrics group by host, metric").show()

// Read a table from Kudu

//    val kuduContext = new KuduContext("172.20.32.162:7051", spark.sparkContext)
//
//    kuduContext.createTable(
//      "metrics2", df.schema, Seq("host","metric","timestamp"),
//      new CreateTableOptions()
//        .setNumReplicas(1)
//        .addHashPartitions(List("host","metric","timestamp").asJava, 3))

//    // Insert data
//    kuduContext.insertRows(df, "test_table")
//
//    // Delete data
//    kuduContext.deleteRows(filteredDF, "test_table")
//
//    // Upsert data
//    kuduContext.upsertRows(df, "test_table")
//
//    // Update data
//    val alteredDF = df.select("id", $"count" + 1)
//    kuduContext.updateRows(filteredRows, "test_table"
//
//      // Data can also be inserted into the Kudu table using the data source, though the methods on KuduContext are preferred
//      // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert in the options map
//      // NB: Only mode Append is supported
//      df.write.options(Map("kudu.master"-> "kudu.master:7051", "kudu.table"-> "test_table")).mode("append").kudu
//
//    // Check for the existence of a Kudu table
//    kuduContext.tableExists("another_table")
//
//    // Delete a Kudu table
//    kuduContext.deleteTable("unwanted_table")


  }

}


