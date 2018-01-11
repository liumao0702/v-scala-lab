package engineer.zhangwei.basic

/**
  * Created by ZhangWei on 2018/1/10.
  */
object caseExample {
    def bigData(data:String): Unit ={
      data match {
        case "Spark" => println("Wow!")
        case "Hadoop" => println("OK!")
        case _ => println("Whatever!")
      }
    }

  def main(args: Array[String]): Unit = {
    bigData("Hadoop")
  }
}
