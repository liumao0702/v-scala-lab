package engineer.zhangwei.spark.ecode

import java.io.{BufferedInputStream, InputStream}
import java.sql
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import com.mysql.jdbc.Connection
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

/**
  * Created by ZhangWei on 2017/6/23.
  * 用于对当月ECODE申请数量的统计和下月ECODE申请数量的预测
  * args(0):统计开始年月（包含），如：2016-05
  * args(1):统计结束年月（包含），如：2017-04
  * args(2):预测年月，如：2017-05
  * args(3):运行方式，默认为Local
  */
object EcodeApplyNumPredict {
  def main(args: Array[String]): Unit = {

    val properties: Properties = new Properties
    val in: InputStream = getClass.getResourceAsStream("ecode.properties")
    properties.load(new BufferedInputStream(in))

    val appNamePredictForIndustry = properties.getProperty("ecode.appNamePredictForIndustry")
    val appNamePredictForCompany =  properties.getProperty("ecode.appNamePredictForCompany")
    val appNamePredictForEcodeApply =  properties.getProperty("ecode.appNamePredictForEcodeApply")
    val driverName =  properties.getProperty("ecode.driverName")
    val zookeeperUrl = properties.getProperty("ecode.zookeeperUrl")
    val tableEcodeApply = properties.getProperty("ecode.tableEcodeApply")
    val tableEcodeRegisterComuser = properties.getProperty("ecode.tableEcodeRegisterComuser")
    val tableEcodeRegisterIndustry = properties.getProperty("ecode.tableEcodeRegisterIndustry")

    val resultDriverName= properties.getProperty("ecode.resultDriverName")
    val resultUrl = properties.getProperty("ecode.resultUrl")
    val resultUserName= properties.getProperty("ecode.resultUserName")
    val resultTableName = properties.getProperty("ecode.resultTableName")
    val resultPasswd= properties.getProperty("ecode.resultPasswd")
    val resultProperties:Properties = new Properties()
    resultProperties.setProperty("user",resultUserName)
    resultProperties.setProperty("password",resultPasswd)
    resultProperties.setProperty("driver",resultDriverName)


    var statisticsMonth_START = args(0)
    var statisticsMonth_END = args(1)
    var predictMonth = args(2)

    var masterUrl = "local[1]"

    if(args.length > 3) {
      masterUrl = args(3)
    }

    val spark = SparkSession.builder
      .master(masterUrl)
      .appName("EcodeStatic"+statisticsMonth_START+"-"+statisticsMonth_END+";EcodePredict"+predictMonth )
      .getOrCreate()

    //EcodeApply
    spark.read
      .format(driverName)
      .option("table",tableEcodeApply)
      .option("zkUrl", zookeeperUrl)
      .load()
      .cache()
      .createOrReplaceTempView(tableEcodeApply)

    //RegisterComuser
    spark.read
      .format(driverName)
      .option("table",tableEcodeRegisterComuser)
      .option("zkUrl", zookeeperUrl)
      .load()
      .cache()
      .createOrReplaceTempView(tableEcodeRegisterComuser)

    //RegisterIndustry
    spark.read
      .format(driverName)
      .option("table",tableEcodeRegisterIndustry)
      .option("zkUrl", zookeeperUrl)
      .load()
      .createOrReplaceTempView(tableEcodeRegisterIndustry)

    val getUUID = udf{(whatever:AnyVal)=>
      UUID.randomUUID.toString.replace("-","")
    }

    val diffMonth = udf{(inputMonth:String)=>
      var period:String=""
      var calbase:Calendar =Calendar.getInstance()
      var calinput:Calendar =Calendar.getInstance()
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      calbase.setTime(argDateFormat.parse(predictMonth))
      calinput.setTime(argDateFormat.parse(inputMonth))
      val monthsBetween = (calinput.get(Calendar.YEAR) - calbase.get(Calendar.YEAR)) * 12 + (calinput.get(Calendar.MONTH) - calbase.get(Calendar.MONTH))
      monthsBetween
    }

    val doubleToInt = udf{(inputDouble:Double)=>
      val outputInt = inputDouble.toInt
      outputInt
    }
    /***********************************************统计信息**************************************************************/

    while (statisticsMonth_START <= statisticsMonth_END){
    println(statisticsMonth_START)
    val monthStart = getMonthStart(statisticsMonth_START)
    val monthEnd = getMonthEnd(statisticsMonth_START)

    val statisticsTableForECODE = spark.sql("SELECT '" +statisticsMonth_START+ "' AS YEAR_MONTH, SUM(a.ECODE_APPLY_NUMBER) AS ECODE_APPLY_NUM_TRUE, " +
        "'0' AS INDUSTRY_ID, '编码中心' AS INDUSTRY_NAME,'E' AS FLAG ,'"+getCurrentTime+"' AS CREATE_TIME " +
        "FROM "+ tableEcodeApply +" a " +
        "WHERE a.ECODE_APPLY_TIME >= '"+ monthStart +"' AND a.ECODE_APPLY_TIME <= '"+ monthEnd +"'")
        .withColumn("ID", getUUID(col("YEAR_MONTH")))
    //statisticsTableForECODE.show()
    statisticsTableForECODE.write.mode(SaveMode.Append).jdbc(resultUrl,resultTableName , resultProperties)

    val statisticsTableForIndustry = spark.sql("" +
      s"SELECT b.COMPANY_INDUSTRY_CODE AS INDUSTRY_ID, b.COMPANY_INDUSTRY_NAME AS INDUSTRY_NAME, '" +statisticsMonth_START+ "' AS YEAR_MONTH, " +
      s"SUM(a.ECODE_APPLY_NUMBER) AS ECODE_APPLY_NUM_TRUE, " +
      s"'I' AS FLAG, '"+getCurrentTime+"' AS CREATE_TIME " +
      s"FROM "+ tableEcodeApply +" a JOIN "+ tableEcodeRegisterComuser + " b ON a.ECODE_APPLY_COMPANYID=b.ID " +
      s"WHERE a.ECODE_APPLY_TIME >= '"+ monthStart +"' AND a.ECODE_APPLY_TIME <= '"+ monthEnd +"' " +
      s"GROUP BY b.COMPANY_INDUSTRY_CODE, b.COMPANY_INDUSTRY_NAME ORDER BY b.COMPANY_INDUSTRY_NAME").withColumn("ID", getUUID(col("YEAR_MONTH")))

    //statisticsTableForIndustry.show()
    statisticsTableForIndustry.write.mode(SaveMode.Append).jdbc(resultUrl,resultTableName , resultProperties)

    val statisticsTableForCompany = spark.sql("" +
      s"SELECT a.ECODE_APPLY_COMPANYID AS COMPANY_ID, b.COMPANY_NAME AS COMPANY_NAME, b.COMPANY_INDUSTRY_CODE AS INDUSTRY_ID, b.COMPANY_INDUSTRY_NAME AS INDUSTRY_NAME, '" +statisticsMonth_START+ "' AS YEAR_MONTH, " +
      s"SUM(a.ECODE_APPLY_NUMBER) AS ECODE_APPLY_NUM_TRUE, " +
      s"'C' AS FLAG, '"+getCurrentTime+"' AS CREATE_TIME " +
      s"FROM "+ tableEcodeApply +" a JOIN "+ tableEcodeRegisterComuser + " b ON a.ECODE_APPLY_COMPANYID=b.ID " +
      s"WHERE a.ECODE_APPLY_TIME >= '"+ monthStart +"' AND a.ECODE_APPLY_TIME <= '"+ monthEnd +"' " +
      s"GROUP BY a.ECODE_APPLY_COMPANYID, b.COMPANY_NAME, b.COMPANY_INDUSTRY_CODE, b.COMPANY_INDUSTRY_NAME ORDER BY b.COMPANY_NAME").withColumn("ID", getUUID(col("YEAR_MONTH")))

    //statisticsTableForCompany.show()
    statisticsTableForCompany.write.mode(SaveMode.Append).jdbc(resultUrl,resultTableName , resultProperties)
    statisticsMonth_START = addMonth(statisticsMonth_START)
   }

/***********************************************数据预测**********************************************************************/

    val trainTable = spark.read.jdbc(resultUrl,resultTableName,resultProperties).cache().createOrReplaceTempView("traindata")

    val companyIdList = spark.sql("SELECT DISTINCT(COMPANY_ID) FROM traindata WHERE  FLAG = 'C'").collect()
    val industryIdList = spark.sql("SELECT DISTINCT(INDUSTRY_ID) FROM traindata WHERE  FLAG = 'I'").collect()
    val ecodeIdList = spark.sql("SELECT DISTINCT(INDUSTRY_ID) FROM traindata WHERE  FLAG = 'E'").collect()

    companyIdList.foreach(companyId => {
      val predictCondition = " AND COMPANY_ID = '"+companyId.get(0).toString()+"' AND FLAG = 'C'"
        predictNum(spark,diffMonth,getUUID,doubleToInt,predictMonth,predictCondition,resultUrl,"ECODE_PREDICT",resultProperties)
      }
    )

    industryIdList.foreach(industryId => {
      val predictCondition = " AND INDUSTRY_ID = '"+industryId.get(0).toString()+"' AND FLAG = 'I'"
      predictNum(spark,diffMonth,getUUID,doubleToInt,predictMonth,predictCondition,resultUrl,"ECODE_PREDICT",resultProperties)
      }
    )
      ecodeIdList.foreach(ecodeId => {
      val predictCondition = " AND INDUSTRY_ID = '"+ecodeId.get(0).toString()+"' AND FLAG = 'E'"
      predictNum(spark,diffMonth,getUUID,doubleToInt,predictMonth,predictCondition,resultUrl,"ECODE_PREDICT",resultProperties)
      }
    )

    //updateForAll(resultDriverName,resultUrl,resultUserName,resultPasswd)
  }

  //根据参数获取该月第一天
  def getMonthStart(arg:String*):String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance()
    if(arg.length > 0){
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      cal.setTime(argDateFormat.parse(arg.head))
    }
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(Calendar.DATE, 1)
    cal.set(Calendar.HOUR_OF_DAY,0)
    cal.set(Calendar.MINUTE,0)
    cal.set(Calendar.SECOND,0)
    period=df.format(cal.getTime())//本月第一天
    period
  }

  //根据参数获取该月最后一天
  def getMonthEnd(arg:String*):String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    if(arg.length > 0){
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      cal.setTime(argDateFormat.parse(arg.head))
    }
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    cal.set(Calendar.HOUR_OF_DAY,23)
    cal.set(Calendar.MINUTE,59)
    cal.set(Calendar.SECOND,59)
    period=df.format(cal.getTime())//本月最后一天
    period
  }

  //根据参数月份加一
  def addMonth(arg:String*):String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    if(arg.length > 0){
      val argDateFormat = new SimpleDateFormat("yyyy-MM")
      cal.setTime(argDateFormat.parse(arg.head))
    }
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM");
    cal.add(Calendar.MONTH, 1)
    period=df.format(cal.getTime())
    period
  }

  def getCurrentTime:String = {
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    period=df.format(cal.getTime())//本月最后一天
    period
  }

  def predictNum(spark:SparkSession
                 ,diffMonth:UserDefinedFunction
                 ,getUUID:UserDefinedFunction
                 ,doubleToInt:UserDefinedFunction
                 ,predictMonth:String
                 ,predictCondition:String
                 ,resultUrl:String
                ,resultTableName:String
                ,resultProperties:Properties
                ):Unit = {
    //构建模型
    val trainData = spark
      .sql( "select ID,YEAR_MONTH,COMPANY_ID,INDUSTRY_ID,FLAG,CAST(ECODE_APPLY_NUM_TRUE AS Double),'"+getCurrentTime+"' AS PREDICT_TIME from traindata WHERE 1=1 "+predictCondition+" ORDER BY YEAR_MONTH")
      .withColumn("DIFFMONTH",diffMonth(col("YEAR_MONTH")))

    //trainData.show()
    val colArray = Array("DIFFMONTH")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF = assembler.transform(trainData)
    val lrModel = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("ECODE_APPLY_NUM_TRUE")
      .setFitIntercept(true)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .fit(vecDF)
    lrModel.extractParamMap()
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //    val predictions = lrModel.transform(vecDF)
    //    println(predictions)

    //使用模型
    spark
      .sql( "select COMPANY_ID,INDUSTRY_ID,FLAG,'"+predictMonth+"' AS YEAR_MONTH,'"+getCurrentTime+"' AS PREDICT_TIME from traindata WHERE 1=1 "+predictCondition+" LIMIT 1 ")
      .withColumn("DIFFMONTH",diffMonth(col("YEAR_MONTH")))
      .withColumn("ID", getUUID(col("YEAR_MONTH")))
      .createOrReplaceTempView("testtable")

    val mydf =spark.sql( "select ID,YEAR_MONTH,COMPANY_ID,INDUSTRY_ID,FLAG,DIFFMONTH,PREDICT_TIME from testtable")
    //mydf.printSchema()
    val vectestDF = assembler.transform(mydf).drop("DIFFMONTH")
    val finaldf = lrModel.transform(vectestDF)
   //finaldf.printSchema()
    val resultdf = finaldf.withColumnRenamed("prediction","PREDICT_NUM").drop("features")
    //resultdf.show()
    resultdf.withColumn("PREDICT_NUM",doubleToInt(col("PREDICT_NUM"))).write.mode(SaveMode.Append).jdbc(resultUrl,resultTableName,resultProperties)
  }

  def updateForAll(driver:String,url:String,username:String,password:String):Unit = {
    var connection:sql.Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    statement.executeUpdate("UPDATE ECODE_APPLY_STATISTICS SET ECODE_APPLY_NUM_PRED = 2")
  }
}
