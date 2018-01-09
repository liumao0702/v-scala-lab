package engineer.zhangwei.storage.hbase



import java.io.{BufferedInputStream, InputStream}
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Row, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ZhangWei on 2017/11/22.
  *
  */
object HbaseSparkBulkload {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    val spark = SparkSession.builder.config(sparkConf) .appName("Load Data") .getOrCreate()
    val inputCbxxParquetPath = "hdfs://train01.haiyi.com:8020/usr/geodedata4000W"
    val df  = spark.read.parquet(inputCbxxParquetPath)

    val schemaList = df.schema.fields.map(structerType => structerType.name).toList

    val rdd = df.rdd.mapPartitions{rows=>
      rows.flatMap{row=>
        row.toSeq.zipWithIndex.map{x=>
          if(x._1 != null){
            val kv:KeyValue = new KeyValue(Bytes.toBytes(row.get(0).toString) , "cf1".getBytes() , schemaList(x._2).getBytes() , Bytes.toBytes(x.toString))
            (new ImmutableBytesWritable(Bytes.toBytes(row.get(0).toString + schemaList(x._2).toString)),kv)
          }else{
            null
          }
        }
      }
    }

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val tablename = "TEST_BULK_LOAD"
    val tableName:TableName = TableName.valueOf(tablename)
    val table:Table = connection.getTable(tableName)
    val admin:Admin = connection.getAdmin

   lazy val job = Job.getInstance()
    job.setMapOutputKeyClass( classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass( classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job,table)
    val regionLocator = connection.getRegionLocator(TableName.valueOf(tablename))
    HFileOutputFormat2.configureIncrementalLoad(job,table,regionLocator)

    // save HFILE on HDFS
//    rdd.filter(_!=null).sort.sortByKey(true,30) .saveAsNewAPIHadoopFile("bulk_load_tmp/xxx" , classOf[ImmutableBytesWritable] , classOf[KeyValue] , classOf[HFileOutputFormat2] ,conf)
    //Bulk load HFILE to HBase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("bulk_load_tmp/xxx"),admin,table,regionLocator)
  }
}


