package engineer.zhangwei.storage.hbase

import org.apache.hadoop.hbase.{NamespaceDescriptor, _}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.regionserver.{IncreasingToUpperBoundRegionSplitPolicy, RegionSplitPolicy}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

/**
  * Created by ZhangWei on 2017/9/25.
  */
object HbaseAdminExample {
  val logger:Logger = Logger.getLogger("hbaseLog")
  def getConnection:Connection = {
    val conf = HBaseConfiguration.create()
    ConnectionFactory.createConnection(conf)
  }

  def getAdmin:Admin = {
    val connection:Connection = getConnection
    connection.getAdmin
  }

  def createTableName(tableName:String):TableName = {
    TableName.valueOf(tableName)
  }

  def deleteTableDemo(table: String):Unit = {
    val admin:Admin = getAdmin
    val tableName:TableName = createTableName(table)
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  def createTable(nameSpace:String ,table:String):Unit = {
    val admin:Admin = getAdmin
    val tableName:TableName = TableName.valueOf(nameSpace,table)
    val tableDescriptor = new HTableDescriptor(tableName)
    tableDescriptor.addFamily(new HColumnDescriptor("c1"))
    admin.createTable(tableDescriptor)
    val tableAvailable = admin.isTableAvailable(tableName)
    logger.info("tableAvailable("+table+") = "+tableAvailable)
  }

  def alterTable(nameSpace:String ,table:String):Unit = {
    val admin:Admin = getAdmin
    val tableName:TableName = TableName.valueOf(nameSpace,table)
    val tableDescriptor = admin.getTableDescriptor(tableName)
//    tableDescriptor.setDurability(Durability.SKIP_WAL)
//    tableDescriptor.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy")
//    tableDescriptor.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy")
 //    tableDescriptor.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy")
//    tableDescriptor.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.DelimitedKeyPrefixRegionSplitPolicy")
    tableDescriptor.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy")
    admin.modifyTable(tableName, tableDescriptor)
  }

  def createNameSpace(nameSpace:String): Unit = {
    val admin:Admin = getAdmin
    val namespaceDescriptor:NamespaceDescriptor = NamespaceDescriptor.create(nameSpace).build()
    admin.createNamespace(namespaceDescriptor)
    logger.info("NameSpace("+nameSpace+") created!")
  }

  def putExample(nameSpace:String ,table:String):Unit = {
    val connection:Connection = getConnection
    val tableName:TableName = TableName.valueOf(nameSpace,table)
    val tbl:Table = connection.getTable(tableName)
    val put:Put = new Put(Bytes.toBytes("2451889"))
    put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("ss_sold_time_sk"),Bytes.toBytes("60920"))
    //关闭WAL
    put.setDurability(Durability.SKIP_WAL)
    tbl.put(put)
    tbl.close()
    connection.close()
  }


  def createTableWithRegions(nameSpace:String ,table:String):Unit = {
    val admin:Admin = getAdmin
    val tableName:TableName = TableName.valueOf(nameSpace,table)
    val tableDescriptor = new HTableDescriptor(tableName)
    tableDescriptor.addFamily(new HColumnDescriptor("c1"))
    val regions = Array[Array[Byte]](// co CreateTableWithRegionsExample-5-Regions Manually create region split keys.
      Bytes.toBytes("A"), Bytes.toBytes("D"), Bytes.toBytes("G"), Bytes.toBytes("K"), Bytes.toBytes("O"), Bytes.toBytes("T"))


    admin.createTable(tableDescriptor, regions)
    //    admin.createTable(tableDescriptor, Bytes.toBytes(1L), Bytes.toBytes(100L), 10)
    val tableAvailable = admin.isTableAvailable(tableName)
    logger.info("tableAvailable("+table+") = "+tableAvailable)
  }

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    print(conf.get("hbase.hstore.blockingStoreFiles"))


//    createNameSpace("htpc")
//      createTable("htpc","test_store_sales")
//      createTable("htpc","test_catalog_returns")
      createTable("htpc","test_customer")
//    createTableWithRegions("htpc","split_region")
//      deleteTableDemo("cbxx_bulk")
//      deleteTableDemo("htpc:test_catalog_returns")

//    alterTable("htpc","test_catalog_returns")
    //  createNameSpace("htpc")
//      putExample("htpc","store_sales")

  }
}
