package engineer.zhangwei.storage.hbase.util

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

/**
  * Created by ZhangWei on 2017/9/26.
  */
object HbaseUtils {
  val logger:Logger = Logger.getLogger("hbaseLog")


  def convertArrayToHadoopPut(schemaString:Array[String],array: Array[String],pkIndex:Array[Int]):(ImmutableBytesWritable,Put) = {
    val pkString = combineRowKey(array,pkIndex)
    val put = new Put(Bytes.toBytes(pkString))
    for (p <- schemaString.indices){
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes(schemaString(p)), Bytes.toBytes(array(p)))
    }
    (new ImmutableBytesWritable, put)
  }

  /*
  * 根据联合主键索引，拼接主键字段
  * */
  def combineRowKey(array: Array[String],pkIndex:Array[Int]):String = {
    var pkString = ""
    for(x <- pkIndex){pkString = pkString + array(x)}
    pkString
  }


}
