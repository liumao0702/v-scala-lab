package engineer.zhangwei.storage.hbase

import java.io.{BufferedInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.log4j.Logger
import engineer.zhangwei.storage.hbase.util.HbaseUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.phoenix.spark._

import scala.math
/**
  * Created by ZhangWei on 2017/9/28.
  */
object HbaseWriteTest {
  val logger:Logger = Logger.getLogger("hbaseLog")
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties
    val in: InputStream = getClass.getClassLoader.getResourceAsStream("storage-test.properties")
    properties.load(new BufferedInputStream(in))
    val inputHdfsPath =  properties.getProperty("input.hdfs.path")
    val inputTableSchema =  properties.getProperty("input.table.schema")
    val outputTableNameSpace = properties.getProperty("output.table.namespace")
    val outputTableName = properties.getProperty("output.table.name")
    val outputTableFullName = properties.getProperty("output.table.fullname")
    val pkIndex = properties.getProperty("table.pk.index")
    val repartitionNum = properties.getProperty("input.repartition.num").toInt

    val pkIndexArray:Array[Int] = pkIndex.split(",",-1).map(_.toInt)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStart = new Date()
    logger.info("Job Start Time:"+sdf.format(dateStart))

    val spark = SparkSession.builder
      .appName("Load Data")
      .getOrCreate()

    val inputHdfsPathI = "hdfs://bg-demo-01.haiyi.com:8020/data/tpc-ds100/inventory.dat"
    val inputHdfsPathS= "hdfs://bg-demo-01.haiyi.com:8020/data/tpc-ds100/store_sales.dat"
      val inputTableSchemaI = "inv_date_sk,inv_item_sk,inv_warehouse_sk,inv_quantity_on_hand"
      val inputTableSchemaS = "ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit"
      val  outputTableFullNameI= "tpc.inventory"
      val  outputTableFullNameS= "tpc.store_sales"
//    writeHbaseUsingPut(spark,inputHdfsPath,inputTableSchema,outputTableNameSpace,outputTableName,pkIndexArray)
//    writePhoenixUsingDataFrame(spark,inputHdfsPath,inputTableSchema,"tpc.catalog_returns","172.20.32.211:2181:/hbase-unsecure")
    writePhoenixUsingRDDInventory(spark,inputHdfsPathI,inputTableSchemaI,outputTableFullNameI,"172.20.32.211:2181:/hbase-unsecure")
    writePhoenixUsingRDDStoreSales(spark,inputHdfsPathS,inputTableSchemaS,outputTableFullNameS,"172.20.32.211:2181:/hbase-unsecure")

    val dateFinish = new Date()
    logger.info("Job Finished Time:"+sdf.format(dateFinish))
  }

  def writeHbaseUsingHadoopDataset(spark:SparkSession, inputHdfsPath:String, inputTableSchema:String, outputTableNameSpace:String, outputTableName:String, pkIndex:Array[Int]): Unit = {
    val inputRdd = spark.sparkContext
      .textFile(inputHdfsPath)
      .map(_.split("\\|", -1))  //-1参数表示不要忽略最后为空的split 机会

    val inputTableSchemaArray = inputTableSchema.split(",")
    def convertToTestStoreSales(array: Array[String]) = HbaseUtils.convertArrayToHadoopPut(inputTableSchemaArray,array,pkIndex)

    val conf = HBaseConfiguration.create()
    val jobConf=new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,outputTableNameSpace+":"+outputTableName)

    val localData =inputRdd.map(p=>convertToTestStoreSales(p))
    localData.saveAsHadoopDataset(jobConf)
  }

  def writeHbaseUsingPut(spark:SparkSession, inputHdfsPath:String, inputTableSchema:String, outputTableNameSpace:String, outputTableName:String, pkIndex:Array[Int]):Unit = {
    val inputRdd = spark.sparkContext
      .textFile(inputHdfsPath)
      .map(_.split("\\|", -1))

    val inputTableSchemaArray = inputTableSchema.split(",")

    inputRdd.map { r =>
      val pkString = HbaseUtils.combineRowKey(r,pkIndex)
      val put = new Put(Bytes.toBytes(pkString))
      r.zip(inputTableSchemaArray).foreach { x =>
        put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes(x._2), Bytes.toBytes(x._1))
      }
      put
    }.foreachPartition { partitionIter =>
      val tableName:TableName = TableName.valueOf(outputTableNameSpace,outputTableName)
      val conf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(tableName)
      val putList = new java.util.ArrayList[Put]()
      while (partitionIter.hasNext) {
        putList.add(partitionIter.next())
        if (putList.size() == 100000) {
          table.put(putList)
          putList.clear()
        }
      }
      table.put(putList)
      table.close()
      connection.close()
    }
  }

  def writePhoenixUsingPhoenixData(spark:SparkSession, inputTableName:String, outputTableName:String, zkString:String):Unit = {
    val df =   spark.read
      .format("org.apache.phoenix.spark")
      .option("table",inputTableName)
      .option("zkUrl", zkString)
      .load()

    df.saveToPhoenix(outputTableName)
  }

  def writePhoenixUsingDataFrame(spark:SparkSession, inputHdfsPath:String, inputTableSchema:String ,outputTableName:String, zkString:String):Unit = {

    val inputSchema = inputTableSchema.split(",").map("0." + _.toUpperCase()).mkString(",")
//    val df = spark.read.
//      textFile(inputHdfsPath).map(_.split("\\|")).map { array =>
//      //      Row()
//    }
  }
    def writePhoenixUsingRDDInventory(spark:SparkSession, inputHdfsPath:String, inputTableSchema:String ,outputTableName:String, zkString:String):Unit = {
      import spark.implicits._
      val inputSchema = inputTableSchema.split(",").map(_.toUpperCase()).toSeq
      val df = spark.read.
        textFile(inputHdfsPath).map(_.split("\\|", -1)).map{data =>
        inventory(data(0),data(1),data(2),data(3))
      }.rdd


//      val dataSet = List((2450926,222,333,222))
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      df.saveToPhoenix(
        outputTableName,
        inputSchema,
        zkUrl = Some(zkString)
      )
//      .map(_.split("\\|", -1))
//      .toDF()
//      .show()
//      .map(_.split("\\|", -1)).toDF(inputSchema).show()
//    val conf = HBaseConfiguration.create()
//    df.saveToPhoenix(outputTableName,conf)
  }

  def writePhoenixUsingRDDStoreSales(spark:SparkSession, inputHdfsPath:String, inputTableSchema:String ,outputTableName:String, zkString:String):Unit = {
    import spark.implicits._
    val inputSchema = inputTableSchema.split(",").map(_.toUpperCase()).toSeq
    val df = spark.read.
      textFile(inputHdfsPath).map(_.split("\\|", -1)).map{data =>
      store_sales(data(0),data(1),data(2),data(3),data(4),data(5),data(6),data(7),data(8),data(9),data(10),data(11),data(12),data(13),data(14),data(15),data(16),data(17),data(18),data(19),data(20),data(21),data(22))
    }.rdd


    //      val dataSet = List((2450926,222,333,222))
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    df.saveToPhoenix(
      outputTableName,
      inputSchema,
      zkUrl = Some(zkString)
    )
    //      .map(_.split("\\|", -1))
    //      .toDF()
    //      .show()
    //      .map(_.split("\\|", -1)).toDF(inputSchema).show()
    //    val conf = HBaseConfiguration.create()
    //    df.saveToPhoenix(outputTableName,conf)
  }
  case class catalog_returns(cr_returned_date_sk:String,cr_returned_time_sk:String,cr_item_sk:String,cr_refunded_customer_sk:String,cr_refunded_cdemo_sk:String,cr_refunded_hdemo_sk:String,cr_refunded_addr_sk:String,cr_returning_customer_sk:String,cr_returning_cdemo_sk:String,cr_returning_hdemo_sk:String,cr_returning_addr_sk:String,cr_call_center_sk:String,cr_catalog_page_sk:String,cr_ship_mode_sk:String,cr_warehouse_sk:String,cr_reason_sk:String,cr_order_number:String,cr_return_quantity:String,cr_return_amount:String,cr_return_tax:String,cr_return_amt_inc_tax:String,cr_fee:String,cr_return_ship_cost:String,cr_refunded_cash:String,cr_reversed_charge:String,cr_store_credit:String,cr_net_loss:String)
  case class inventory(inv_date_sk:String,inv_item_sk:String,inv_warehouse_sk:String,inv_quantity_on_hand:String)
  case class store_sales(ss_sold_date_sk:String,ss_sold_time_sk:String,ss_item_sk:String,ss_customer_sk:String,ss_cdemo_sk:String,ss_hdemo_sk:String,ss_addr_sk:String,ss_store_sk:String,ss_promo_sk:String,ss_ticket_number:String,ss_quantity:String,ss_wholesale_cost:String,ss_list_price:String,ss_sales_price:String,ss_ext_discount_amt:String,ss_ext_sales_price:String,ss_ext_wholesale_cost:String,ss_ext_list_price:String,ss_ext_tax:String,ss_coupon_amt:String,ss_net_paid:String,ss_net_paid_inc_tax:String,ss_net_profit:String)

}
