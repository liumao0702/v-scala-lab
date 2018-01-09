package engineer.zhangwei.spark.sparkts

import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by ZhangWei on 2017/7/19.
  */
object SparktsTest {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
        .master("local[1]")
      .appName("sparktsTest" )
      .getOrCreate()


    val lines = scala.io.Source.fromFile("E:/R_ARIMA_DataSet1.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1,0,0,ts)
//    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 1)
    print(Math.abs(forecast.toArray.last))
//    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))

//
//    spark.read.json("file:///E:/people.json").createTempView("data")
//    val tmpDF = spark.sql("select MAX_TEMPRATURE,CLIMATE_ID,MIN_TEMPRATURE,AVERAGE_TEMPRATURE from data")
//    tmpDF.show()
//    val labelAndVectorData = tmpDF.rdd.map { r =>
//      val vector = Vectors.dense(r.toSeq.toArray.takeRight(3).map(a=>a.toString.toDouble))
//      val arrLabel = r.toSeq.toArray.take(1)
//      (arrLabel.mkString(","),vector)
//    }
//    labelAndVectorData
//
//
//
//
//    val predictData = labelAndVectorData.map{a=>
//      println(a._2.toString)
//      val model = ARIMA.autoFit(a._2)
//      println("coefficients: " + model.coefficients.mkString(","))
//      val res = model.forecast(a._2,1)
//      val predict = res.toArray.takeRight(1)
//      val pre = predict.map{a=>
//        f"$a%1.2f"
//      }
//      a._1+","+a._2.toArray.mkString(",")+","+pre.mkString(",")
//
//    }
//    println("结果集---------------------------------------------")
//    predictData.collect().foreach(println)
  }

}

