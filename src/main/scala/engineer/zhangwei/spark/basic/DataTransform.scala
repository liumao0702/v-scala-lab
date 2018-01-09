package engineer.zhangwei.spark.basic

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
/**
  * Created by ZhangWei on 2017/6/29.
  */
object DataTransform {
  def main(args: Array[String]): Unit = {

/**********************************参数及初始化************************************************/
    val kuduMaster = "172.20.32.162:7051"
    val spark = SparkSession.builder
      .appName("Transform1")
//      .config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.sql.parquet.compression.codec","uncompressed")
//     .config("spark.sql.parquet.compression.codec","snappy")
//      .master("local[1]")
      .getOrCreate()



    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

/**********************************公用方法*************************************************/
    val addYear = udf{(inputRowKey:String,addedYear:Int)=>
      val inputYearMonth = inputRowKey.substring(0,7)
      var calinput:Calendar =Calendar.getInstance()
      var caloutput:Calendar =Calendar.getInstance()
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      calinput.setTime(argDateFormat.parse(inputYearMonth))
      calinput.add(Calendar.YEAR,addedYear)
      argDateFormat.format(calinput.getTime())+inputRowKey.substring(7)+"pluspar"
    }
    val getUUID = udf{(whatever:AnyVal)=>
      UUID.randomUUID.toString.replace("-","")
    }
/**********************************原始数据*************************************************/
    //创建DataFrame

    //造了1个亿的数据

   val testJsonPath = "hdfs://172.20.32.211/data/zhangwei/geodedataJson1000W"
   spark.read.json(testJsonPath).cache().createOrReplaceTempView("testdata")
   val queryCondition = " LIMIT 1000000"
   var finalDF = spark.sql("SELECT * FROM testdata WHERE 1=1 "+queryCondition).withColumn("ROWKEY",addYear(col("ROWKEY"),lit(0)))
    for(i <- 1 to 9){
      val tmpDF = spark.sql("SELECT * FROM testdata WHERE 1=1 "+queryCondition).withColumn("ROWKEY",addYear(col("ROWKEY"),lit(i)))
      finalDF = finalDF.union(tmpDF)
    }
    finalDF.write.parquet("hdfs://172.20.32.175/data/1yiParquet_1kw_par")

//    val test1yiParquet = "hdfs://172.20.32.175/data/zhangwei/1yiParquet_1kw_par"
//    spark.read.parquet(test1yiParquet).cache().createOrReplaceTempView("test1yidata")
//    val testDF = spark.sql("SELECT ROWKEY FROM test1yidata WHERE ROWKEY LIKE '%plus'")

    /**********************************空间利用率测试************************************************/
    //Parquet
    //testDF.write.save("hdfs://172.20.32.175/data/zhangwei/1yiParquet_snappy")
    //testDF.write.save("hdfs://172.20.32.175/data/zhangwei/1yiParquet_gzip")
    //testDF.write.save("hdfs://172.20.32.175/data/zhangwei/1yiParquet_uncompressed")
//    kuduTableName = "1yi_NO_COMPRESSION"
//
//    val kuduOptions: Map[String, String] = Map(
//      "kudu.table"  -> kuduTableName,
//      "kudu.master" -> kuduMaster)
//
//
//    spark.read.options(kuduOptions).kudu.createOrReplaceTempView(kuduTableName)
//    val testDF1 = spark.sql(s"SELECT ROWKEY FROM $kuduTableName WHERE ROWKEY LIKE '%plus'")
//
//    kuduTableName = "1yi_NO_COMPRESSION_PAR"
//
//    val kuduOptions2: Map[String, String] = Map(
//      "kudu.table"  -> kuduTableName,
//      "kudu.master" -> kuduMaster)
//
//
//    spark.read.options(kuduOptions2).kudu.createOrReplaceTempView(kuduTableName)
//    val testDF2 = spark.sql(s"SELECT ROWKEY FROM $kuduTableName WHERE ROWKEY LIKE '%plus'")
    //Kudu

//    kuduContext.deleteRows(testDF,"1yi_NO_COMPRESSION_PAR")
//    kuduContext.deleteRows(testDF,"1yi_NO_COMPRESSION")
//    kuduContext.deleteRows(testDF2,"1yi_NO_COMPRESSION_PAR")
//    kuduContext.deleteRows(testDF1,"1yi_NO_COMPRESSION")
//    kuduContext.insertRows(testDF,"1yi_SNAPPY")
//    kuduContext.insertRows(testDF,"1yi_ZLIB")

/**********************************查询测试****************************************************/
//    kuduTableName = "1yi_NO_COMPRESSION"
//
//    val kuduOptions: Map[String, String] = Map(
//      "kudu.table"  -> kuduTableName,
//      "kudu.master" -> kuduMaster)
//
//
//    spark.read.options(kuduOptions).kudu.createOrReplaceTempView(kuduTableName)
//    val num = spark.sql(s"SELECT * FROM $kuduTableName").count()
//    println(num)
/**********************************插入测试****************************************************/
//    val df1=spark.sql(s"SELECT  *  FROM $kuduTableName where area_code='0312' ")
//    val df2=spark.sql(s"SELECT  *  FROM $kuduTableName where area_code='0313' ")
//    val df3=spark.sql(s"SELECT  *  FROM $kuduTableName where area_code='0314' ")
//    val changedDF = df1.union(df2).union(df3).repartition(30)
  //  val changedDF = spark.sql(s"SELECT  ROWKEY,I_A  FROM $kuduTableName where area_code='0312' ")
  //  changedDF.drop(col("I_A")).withColumn("I_A",getUUID(col("ROWKEY")))
   // kuduContext.upsertRows(changedDF, kuduTableName)
//    spark.sql(s"SELECT COUNT(*) FROM $kuduTableName ").show()

/**********************************测试开始****************************************************/
//  val kuduTableName = "1yi_NO_COMPRESSION"
//  val kuduTableNamePar = "1yi_NO_COMPRESSION_PAR"
//1. 插入1yi数据
//    val test1yiParquet = "hdfs://172.20.32.175/data/zhangwei/1yiParquet"
//    spark.read.parquet(test1yiParquet).cache().createOrReplaceTempView("test1yiParquet")
//    val yitestDF = spark.sql("SELECT * FROM test1yiParquet")
//    kuduContext.insertRows(yitestDF, kuduTableName)   //21min
//    kuduContext.insertRows(yitestDF, kuduTableNamePar)  //12min
//
//2. 插入1kw未分区数据
//    val test1yiParquet_no_par = "hdfs://172.20.32.175/data/zhangwei/1yiParquet_1000wplus"
//    spark.read.parquet(test1yiParquet_no_par).cache().createOrReplaceTempView("test1yiParquet_no_par")
//    val kwtestDF_no_par = spark.sql("SELECT * FROM test1yiParquet_no_par")
//    kuduContext.insertRows(kwtestDF_no_par, kuduTableName)    //2min
//    kuduContext.insertRows(kwtestDF_no_par, kuduTableNamePar) //2min
//
//3. 插入1kw分区数据
//  val test1yiParquet_par = "hdfs://172.20.32.175/data/zhangwei/1yiParquet_1kw_par"
//    spark.read.parquet(test1yiParquet_par).cache().createOrReplaceTempView("test1yiParquet_par")
//    val kwtestDF_par = spark.sql("SELECT * FROM test1yiParquet_par")
//    kuduContext.insertRows(kwtestDF_par, kuduTableName)   //2.4min
//    kuduContext.insertRows(kwtestDF_par, kuduTableNamePar)  //1.8min

//4. update 1kw分区数据
//    val kwtestDF_par_update = spark.sql("SELECT * FROM test1yiParquet_par").withColumn("POWER_FACTOR_A",getUUID(col("POWER_FACTOR_A")))
//    kuduContext.updateRows(kwtestDF_par_update, kuduTableName)    //2.5min
//    kuduContext.updateRows(kwtestDF_par_update, kuduTableNamePar) //1.6min
//5. upsert 1kw分区数据
//val kwtestDF_par_upsert = spark.sql("SELECT * FROM test1yiParquet_par").withColumn("POWER_FACTOR_B",getUUID(col("POWER_FACTOR_B")))
//    kuduContext.upsertRows(kwtestDF_par_upsert, kuduTableName)    //2.1min
//    kuduContext.upsertRows(kwtestDF_par_upsert, kuduTableNamePar) //1.8min
//6. 删除1kw未分区数据
//    val kwtestDF_no_par_rowkey = spark.sql("SELECT ROWKEY FROM test1yiParquet_no_par")
//    kuduContext.deleteRows(kwtestDF_no_par_rowkey, kuduTableName)   //37s
//    kuduContext.deleteRows(kwtestDF_no_par_rowkey, kuduTableNamePar)  //21s
//7. 删除1kw分区数据
//    val kwtestDF_par_rowkey = spark.sql("SELECT ROWKEY FROM test1yiParquet_par")
//    kuduContext.deleteRows(kwtestDF_par_rowkey, kuduTableName)    //1.1min
//    kuduContext.deleteRows(kwtestDF_par_rowkey, kuduTableNamePar) //39s
//8. Rowkey查询速度
//    val kuduOptions: Map[String, String] = Map(
//      "kudu.table"  -> kuduTableName,
//      "kudu.master" -> kuduMaster)
//    val kuduOptionsPar: Map[String, String] = Map(
//      "kudu.table"  -> kuduTableNamePar,
//      "kudu.master" -> kuduMaster)
//   val df =  spark.read.options(kuduOptions).kudu
//   val dfPar =  spark.read.options(kuduOptionsPar).kudu
//    df.select("I_A").filter("ROWKEY = '2017-01-20 11:45:00#^583225453' ").show()      //3s
//    dfPar.select("I_A").filter("ROWKEY = '2017-01-20 11:45:00#^583225453' ").show()   //2s
//9. 非主键查询速度
//    df.select("I_B").filter("I_A='0.18'").count()   //8s
//    dfPar.select("I_B").filter("I_A='0.18'").count()    //7s


  }
}
