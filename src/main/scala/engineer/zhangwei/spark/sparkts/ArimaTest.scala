package engineer.zhangwei.spark.sparkts

import java.io.{BufferedInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cloudera.sparkts.models.ARIMA
import engineer.zhangwei.spark.ecode.EcodeApplyNumPredict.{getClass, predictNum}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * Created by ZhangWei on 2017/9/16.
  */
object ArimaTest {
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties
    val in: InputStream = getClass.getClassLoader.getResourceAsStream("ecode.properties")
    properties.load(new BufferedInputStream(in))

    val resultDriverName= properties.getProperty("ecode.resultDriverName")
    val resultUrl = properties.getProperty("ecode.resultUrl")
    val resultUserName= properties.getProperty("ecode.resultUserName")
    val resultTableName = properties.getProperty("ecode.resultTableName")
    val resultPasswd= properties.getProperty("ecode.resultPasswd")
    val resultProperties:Properties = new Properties()
    resultProperties.setProperty("user",resultUserName)
    resultProperties.setProperty("password",resultPasswd)
    resultProperties.setProperty("driver",resultDriverName)

    val spark = SparkSession.builder
      .master("local")
      .appName("ArimaTest" )
      .getOrCreate()

    def getCurrentTime:String = {
      var period:String=""
      var cal:Calendar =Calendar.getInstance();
      var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      period=df.format(cal.getTime())//本月最后一天
      period
    }

    val trainTable = spark.read.jdbc(resultUrl,resultTableName,resultProperties).cache().createOrReplaceTempView("traindata")
    val ecodeIdList = spark.sql("SELECT DISTINCT(INDUSTRY_ID) FROM traindata WHERE  FLAG = 'E'")
    val predictCondition = " AND INDUSTRY_ID = '0' AND FLAG = 'E'"
    val testDF = spark.sql( "select ID,YEAR_MONTH,COMPANY_ID,INDUSTRY_ID,FLAG,CAST(ECODE_APPLY_NUM_TRUE AS Double),'"+getCurrentTime+"' AS PREDICT_TIME from traindata WHERE 1=1 "+predictCondition+" ORDER BY YEAR_MONTH")

//    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
//    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
//    println("coefficients: " + arimaModel.coefficients.mkString(","))
//    val forecast = arimaModel.forecast(ts, 20)


  }
}
