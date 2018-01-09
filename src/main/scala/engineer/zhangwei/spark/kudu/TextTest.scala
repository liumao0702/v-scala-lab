package engineer.zhangwei.spark.kudu

import java.io.FileWriter
import java.util.UUID

/**
  * Created by ZhangWei on 2017/6/28.
  */
object TextTest {
  def main(args: Array[String]): Unit = {
    val out = new FileWriter("E://test3.txt",true)
    for (i <- 0 to 10000000)
      out.write(getUUID()+","+getUUID()+","+getUUID()+"\n")
    out.close()
  }

  def getUUID():String={
    UUID.randomUUID.toString.replace("-","")
  }
}
