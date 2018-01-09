package engineer.zhangwei.spark.model

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Calendar

import com.cloudera.sparkts.{DateTimeIndex, MonthFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by ZhangWei on 2017/9/16.
  */
object TimeSeriesTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("SparkTest")
      .getOrCreate()

    val timeCol = "time"
    val valueCol = "data"

    val startYearMonth = "2013-01"
    val endYearMonth = "2014-01"
    val zonedId = ZoneId.systemDefault()

    val trainDataDf = spark.read
      .option("header","true")
      .csv("F://timeSeries.csv")

    val field=Seq(
      StructField(timeCol, TimestampType, nullable = true),
      StructField(timeCol +"Key", StringType, true),
      StructField(valueCol, DoubleType, true)
    )

    def yearMonthToZonedDateTime(yearMonth:String):ZonedDateTime = {
      val calinput:Calendar =Calendar.getInstance()
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      calinput.setTime(argDateFormat.parse(yearMonth))
      ZonedDateTime.of(LocalDateTime.ofInstant(calinput.getTime.toInstant,zonedId),zonedId)
    }

    def loadObservations(initialDF: DataFrame):DataFrame = {
      val rowRdd = initialDF.rdd.map {
        case Row(time, key, data) => {
          val dt = yearMonthToZonedDateTime(time.toString)
          Row(Timestamp.from(dt.toInstant), key.toString, data.toString.toDouble)
        }
      }
      val schema=StructType(field)
      spark.sqlContext.createDataFrame(rowRdd,schema)
    }

    val trainDataKeyDf = trainDataDf.withColumn(timeCol+"Key", trainDataDf.col(valueCol) * 0).select(timeCol, timeCol+"Key", valueCol)

    val zonedDateDataDF = loadObservations(trainDataKeyDf)
    zonedDateDataDF.show()
    val dtIndex:UniformDateTimeIndex = DateTimeIndex.uniformFromInterval(yearMonthToZonedDateTime(startYearMonth), yearMonthToZonedDateTime(endYearMonth),new MonthFrequency(1))
    val trainTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex,zonedDateDataDF, timeCol, timeCol+"key",valueCol)

    val timeSeriesModel=new TimeSeriesModel(1)
    val forecastValue=timeSeriesModel.arimaModelTrain(trainTsrdd)
    print(forecastValue)


  }
}
