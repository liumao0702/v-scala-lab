package engineer.zhangwei.spark.ecode

import java.util.Properties

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StringType, StructField}



/**
  * Created by ZhangWei on 2017/6/21.
  */
class TrendsPredict {
  object SparkMl {
    def main (args: Array[String]) {
      val spark: SparkSession = SparkSession.builder().appName("DF_COMPUTE_V2.1").getOrCreate()
      val prePath= "E://test.txt"
      //    val spark: SparkSession = SparkSession.builder().appName("DF_COMPUTE_V2.1").config("spark.sql.warehouse.dir","file:///F:/svn/PROJECT_RTDFJS/trunk/12Source/dfCompute_v.2.1.0/spark-warehouse")
      //      .getOrCreate()
      //
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://192.168.0.75/ecode"
      val tablename = "ECODE_PREDICT"
      val username = "root"
      val password = "ancc,,..//ecode"
      val properties: Properties = new Properties()
      properties.setProperty("user",username)
      properties.setProperty("password",password)
      properties.setProperty("driver",driver)



      val trainTable = spark.read.jdbc(url,tablename,properties).createOrReplaceTempView("traindata")
      val trainData = spark.sql( "select ID,CAST(YEARMONTH AS Double),CAST(TRUE_NUM AS Double) from traindata")
      val colArray = Array("YEARMONTH")
      val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
      val vecDF = assembler.transform(trainData)
      val lr1 = new LinearRegression()
      val lr2 = lr1.setFeaturesCol("features").setLabelCol("TRUE_NUM").setFitIntercept(true)
      val lr3 = lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val lr = lr3
      val lrModel = lr.fit(vecDF)

      lrModel.extractParamMap()
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
      val predictions = lrModel.transform(vecDF)
      println(predictions)


      val schemaString = "ID YEARMONTH"
      val fields = schemaString.split(" ")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val schema = StructType(fields)
      val testRDD = spark.sparkContext.textFile(prePath)
      val rowRDD = testRDD
        .map(_.split(","))
        .map(attributes => Row(attributes(0), attributes(1).trim))
      val testDF = spark.createDataFrame(rowRDD, schema).createOrReplaceTempView("testtable")
      val mydf =spark.sql( "select ID,CAST(YEARMONTH AS Double) from testtable")
      mydf.printSchema()
      val vectestDF = assembler.transform(mydf).drop("YEARMONTH")
      val finaldf = lrModel.transform(vectestDF)
      finaldf.printSchema()
      val get_value = udf{(month:AnyVal)=>
        val monthstr=month.toString
        monthstr.substring(1,monthstr.length-1)

      }
      val resultdf = finaldf.withColumn("YEARMONTH",get_value(col("features"))).withColumnRenamed("prediction","PREDICT_NUM").drop("features")
      resultdf.show()
      resultdf.write.mode("append").jdbc(url,tablename , properties)
    }
  }
}
