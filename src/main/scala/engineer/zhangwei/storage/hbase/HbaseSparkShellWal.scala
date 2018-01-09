package engineer.zhangwei.storage.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{NamespaceDescriptor, _}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by ZhangWei on 2017/11/9.
  */
object HbaseSparkShellWal {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "train03.haiyi.com,train02.haiyi.com,train01.haiyi.com")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val tableName = TableName.valueOf("TEST_XJ")
    val tableDescriptor = admin.getTableDescriptor(tableName)
    tableDescriptor.setDurability(Durability.SKIP_WAL)
    admin.modifyTable(tableName, tableDescriptor)

  }
}
