package engineer.zhangwei.spark.kudu

import java.util.UUID

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._

/**
  * Created by ZhangWei on 2017/6/28.
  */
object KuduText {
  case class Customer(name:String, age:String, city:String)
  def main(args: Array[String]): Unit = {
    //Args Issue
    var masterUrl = "local[1]"

    //Create a SparkContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("KuduTest2")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .master(masterUrl)
      .appName("KuduTest")
      .getOrCreate()

    // Comma-separated list of Kudu masters with port numbers
    val master1 = "node01.cluster.com:7051"
    val master2 = "node02.cluster.com:7051"
    val master3 = "node03.cluster.com:7051"
    val kuduMasters = Seq(master1).mkString(",")

    // Create an instance of a KuduContext
    // Use KuduContext to create, delete, or write to Kudu tables
    val kuduContext = new KuduContext(kuduMasters, spark.sparkContext)

    // Specify a table name
    var kuduTableName = "spark_kudu_tbl_test10000000"

    // Check if the table exists, and drop it if it does
    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }



    // 2. Define a schema
    val kuduTableSchema = StructType(
      //         col name   type     nullable?
      StructField("name", StringType , false) ::
        StructField("age" , StringType, true ) ::
        StructField("city", StringType , true ) :: Nil)
    // 3. Define the primary key
    val kuduPrimaryKey = Seq("name")

    // 4. Specify any further options
    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions.
      setRangePartitionColumns(List("name").asJava).
      setNumReplicas(3)

    // 5. Call create table API
    kuduContext.createTable(
      // Table name, schema, primary key and options
      kuduTableName, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)



    var customers = List(Customer(getUUID(),getUUID(),getUUID()))
    for (i <- 1 to 10000000){
      customers = customers:::List(Customer(getUUID(),getUUID(),getUUID()))
    }

    println(customers.length)
    // Create RDD out of the customers Array
    val customersRDD = spark.createDataFrame(customers)
    val customersDF = customersRDD.toDF()


    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)

    // 2. Insert our customer DataFrame data set into the Kudu table
    kuduContext.insertRows(customersDF, kuduTableName)

    // 3. Read back the records from the Kudu table to see them dumped
    val df = spark.read.options(kuduOptions).kudu
    //customersDF.write.save("file:///E://test")
    df.show()
  }

  def getUUID():String={
    UUID.randomUUID.toString.replace("-","")
  }
}
