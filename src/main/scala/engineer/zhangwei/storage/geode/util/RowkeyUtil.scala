package engineer.zhangwei.storage.geode.util

import java.util.UUID

import scala.util.Random

/**
  * Created by ZhangWei on 2017/7/26.
  */
object RowkeyUtil {

  /**
    * Created by ZhangWei on 2017/7/26.
    */

  def getRanString() : String = {
    val ranstring = UUID.randomUUID.toString.replace("-","").substring(0,15)
    ranstring
  }
    def getRanLong() : Long = {
      val ran = new Random()
      val num = (ran.nextInt(99999999)*10000000L+ran.nextInt(99999999))
      num
    }


}
