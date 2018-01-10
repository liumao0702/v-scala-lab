package engineer.zhangwei.storage.hbase.phoenix

import org.apache.spark.sql.SparkSession

/**
  * Created by ZhangWei on 2017/6/22.
  */
object PhoenixSparkTest {
  def main(args: Array[String]): Unit = {

    /*//读方法一： Load as a DataFrame using the Data Source API

   val spark = SparkSession.builder
     .master("local")
     .appName("PhoenixSparkTest")
     .getOrCreate()

   val df = spark.read
     .format("org.apache.phoenix.spark")
     .options(
       Map("table" -> "TABLE1",
           "zkUrl" -> "jdbc:phoenix:node1.haiyi.com:2181:/hbase-secure:hbase/node1.haiyi.com@EXAMPLE.COM:C:\\Users\\Biguncle\\Desktop\\pho4.10squirrel\\hbase.service.keytab")
     ).load()

   df
     .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
     .select(df("ID"))
     .show
***********************************************************************************************************************************/
    /*//读方法二： Load as a DataFrame directly using a Configuration object

    val configuration = new Configuration()
    // Can set Phoenix-specific settings, requires 'hbase.zookeeper.quorum'
    configuration.set("hbase.zookeeper.quorum","node1.haiyi.com")
    configuration.set("hbase.master.kerberos.principal", "hbase/node1.haiyi.com@EXAMPLE.COM")

    val sc = new SparkContext("local", "phoenix-test")
    val sqlContext = new SQLContext(sc)

    // Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
    val df = sqlContext.phoenixTableAsDataFrame(
      "TABLE1", Array("ID", "COL1"), conf = configuration
    )
    df.show
************************************************************************************************************************************/
    /*//读方法三： Load as an RDD, using a Zookeeper URL

      val sc = new SparkContext("local", "phoenix-test")

      // Load the columns 'ID' and 'COL1' from TABLE1 as an RDD
      val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
        "TABLE1", Seq("ID", "COL1"), zkUrl = Some("jdbc:phoenix:node1.haiyi.com:2181:/hbase-secure:hbase/node1.haiyi.com@EXAMPLE.COM:C:\\Users\\Biguncle\\Desktop\\pho4.10squirrel\\hbase.service.keytab")
      )

      rdd.count()

      val firstId = rdd.first()("ID").asInstanceOf[Long]
      val firstCol = rdd.first()("COL1").asInstanceOf[String]
************************************************************************************************************************************/
    /*//将RDD存入Phoenix

    val sc = new SparkContext("local", "phoenix-test")
    val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID","COL1","COL2"),
        zkUrl = Some("jdbc:phoenix:node1.haiyi.com:2181:/hbase-secure:hbase/node1.haiyi.com@EXAMPLE.COM:C:\\Users\\Biguncle\\Desktop\\pho4.10squirrel\\hbase.service.keytab")
      )
***********************************************************************************************************************************/
    //将DataFrame存入Phoenix
    // Load INPUT_TABLE
    val spark = SparkSession.builder
      .master("local")
      .appName("PhoenixSparkTest")
      .getOrCreate()

    val df = spark.read
      .format("org.apache.phoenix.spark")
      .options(
        Map("table" -> "INPUT_TABLE",
          "zkUrl" -> "jdbc:phoenix:node1.haiyi.com:2181:/hbase-secure:hbase/node1.haiyi.com@EXAMPLE.COM:C:\\Users\\Biguncle\\Desktop\\pho4.10squirrel\\hbase.service.keytab")
      ).load()

    // Save to OUTPUT_TABLE
    df.write
      .format("org.apache.phoenix.spark")
      .option("table","OUTPUT_TABLE")
      .option("zkUrl", "jdbc:phoenix:node1.haiyi.com:2181:/hbase-secure:hbase/node1.haiyi.com@EXAMPLE.COM:C:\\Users\\Biguncle\\Desktop\\pho4.10squirrel\\hbase.service.keytab")
      .mode("overwrite")
      .save()

  }
}
