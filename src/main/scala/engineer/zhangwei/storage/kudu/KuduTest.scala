package engineer.zhangwei.storage.kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._

import collection.JavaConverters._

/**
  * Created by ZhangWei on 2017/6/27.
  */
object KuduTest {
  case class Customer(name:String, age:Int, city:String)


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
    var kuduTableName = "spark_kudu_tbl"

    // Check if the table exists, and drop it if it does
    if (kuduContext.tableExists(kuduTableName)) {
      kuduContext.deleteTable(kuduTableName)
    }

    // 1. Give your table a name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define a schema
    val kuduTableSchema = StructType(
      //         col name   type     nullable?
      StructField("name", StringType , false) ::
        StructField("age" , IntegerType, true ) ::
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


    //Creating a simple dataset, converting it into a DataFrame
    // Define a list of customers based on the case class already defined above
    val customers = Array(
      Customer("jane", 30, "new york"),
      Customer("jordan", 18, "toronto"))

    // Create RDD out of the customers Array
    val customersRDD = spark.createDataFrame(customers)
    val customersDF = customersRDD.toDF()

    //Insert data

    // Define Kudu options used by various operations
    val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Insert our customer DataFrame data set into the Kudu table
    kuduContext.insertRows(customersDF, kuduTableName)

    // 3. Read back the records from the Kudu table to see them dumped
    val df = spark.read.options(kuduOptions).kudu
    df.show()

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Let’s register our customer dataframe as a temporary table so we
    // refer to it in Spark SQL
    customersDF.createOrReplaceTempView("customers")

    // 3. Filter and create a keys-only DataFrame to be deleted from our table
    val deleteKeysDF = spark.sql("select name from customers where age > 20")

    // 4. Delete the rows from our Kudu table
    kuduContext.deleteRows(deleteKeysDF, kuduTableName)

    // 5. Read data from Kudu table
    spark.read.options(kuduOptions).kudu.show

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Define the dataset we want to upsert
    val newAndChangedCustomers = Array(
      Customer("michael", 25, "chicago"),
      Customer("denise" , 43, "winnipeg"),
      Customer("jordan" , 19, "toronto"))

    // 3. Create our dataframe
    val newAndChangedRDD = spark.createDataFrame(newAndChangedCustomers)
    val newAndChangedDF  = newAndChangedRDD.toDF()

    // 4. Call upsert with our new and changed customers DataFrame
    kuduContext.upsertRows(newAndChangedDF, kuduTableName)

    // 5. Show contents of Kudu table
    spark.read.options(kuduOptions).kudu.show

    // 1. Specify your Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Create a DataFrame of updated rows
    val modifiedCustomers = Array(Customer("michael", 25, "toronto"))
    val modifiedCustomersRDD = spark.createDataFrame(modifiedCustomers)
    val modifiedCustomersDF  = modifiedCustomersRDD.toDF()

    // 3. Call update with our new and changed customers DataFrame
    kuduContext.updateRows(modifiedCustomersDF, kuduTableName)

    // 4. Show contents of Kudu table
    spark.read.options(kuduOptions).kudu.show

    // 1. Specify a table name
    kuduTableName = "spark_kudu_tbl"

    // 2. Specify the columns you want to project
    val kuduTableProjColumns = Seq("name", "age")

    // 3. Read table, represented now as RDD
    val custRDD = kuduContext.kuduRDD(sc, kuduTableName, kuduTableProjColumns)

    // We get a RDD[Row] coming back to us. Lets send through a map to pull
    // out the name and age into the form of a tuple
    val custTuple = custRDD.map { case Row(name: String, age: Int) => (name, age) }

    // Print it on the screen just for fun
    custTuple.collect().foreach(println(_))

    // Read our table into a DataFrame - reusing kuduOptions specified
    // above
    val customerReadDF = spark.read.options(kuduOptions).kudu

    // Show our table to the screen.
    customerReadDF.show()

    // Create a small dataset to write (append) to the Kudu table
    val customersAppend = Array(
      Customer("bob", 30, "boston"),
      Customer("charlie", 23, "san francisco"))

    // Create our DataFrame our of our dataset
    val customersAppendDF = spark.createDataFrame(customersAppend).toDF()

    // Specify the table name
    kuduTableName = "spark_kudu_tbl"

    // Call the write method on our DataFrame directly in "append" mode
    customersAppendDF.write.options(kuduOptions).mode("append").kudu

    // See results of our append
    spark.read.options(kuduOptions).kudu.show()

    // Quickly prepare a Kudu table we will use as our source table in Spark
    // SQL.
    // First, some sample data
    val srcTableData = Array(
      Customer("enzo", 43, "oakland"),
      Customer("laura", 27, "vancouver"))

    // Create our DataFrame
    val srcTableDF = spark.createDataFrame(srcTableData).toDF()

    // Register our source table
    srcTableDF.createOrReplaceTempView("source_table")

    // Specify Kudu table name we will be inserting into
    kuduTableName = "spark_kudu_tbl"

    // Register your table as a Spark SQL table.
    // Remember that kuduOptions stores the kuduTableName already as well as
    // the list of Kudu masters.
    spark.read.options(kuduOptions).kudu.createOrReplaceTempView(kuduTableName)

    // Use Spark SQL to INSERT (treated as UPSERT by default) into Kudu table
    spark.sql(s"INSERT INTO TABLE $kuduTableName SELECT * FROM source_table")

    // See results of our insert
    spark.read.options(kuduOptions).kudu.show()


    // Kudu table name
    kuduTableName = "spark_kudu_tbl"

    // Register Kudu table as a Spark SQL temp table
    spark.read.options(kuduOptions).kudu.
      createOrReplaceTempView(kuduTableName)

    // Now refer to that temp table name in our Spark SQL statement
    val customerNameAgeDF = spark.
      sql(s"""SELECT name, age FROM $kuduTableName WHERE age >= 30""")
    customerNameAgeDF.explain()
    // Show the results
    customerNameAgeDF.show()
  }
}

