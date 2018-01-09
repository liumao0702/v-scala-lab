package engineer.zhangwei.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZhangWei on 2017/6/19.
  *
  * 任务1：
  * 看过“Lord of the Rings, The (1978)" 用户和年龄性别分布
  * 应当从user表里获得年龄和性别，rating表里获得连接信息
  */
object MovieUserAnalyser {
  def main(args: Array[String]): Unit = {
    //Args Issue
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"

    if(args.length > 0){
      masterUrl = args(0)
    } else if(args.length > 1){
      dataPath = args(1)
    }

    //Create a SparkContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyser")
    val sc = new SparkContext(conf)

    /**
      * Step 1 Create RDDS
      */

    val DATA_PATH = dataPath
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    val MOVIE_ID = "2116"

    //参数可以是文件或者目录
    val usersRdd = sc.textFile(DATA_PATH + "users.dat")
    val ratingsRdd = sc.textFile(DATA_PATH + "ratings.dat")

    /**
      * Step 2 Extract columns from RDDs
      */

    //从用户文件里获得用户的年龄和性别：users RDD[(userID,(gender,age))]
    val users = usersRdd.map(_.split("::")).map{ x =>
      (x(0),(x(1),x(2)))
    }

    //从rating文件里获得user和movie的关联信息：rating RDD[Array(userID,movieID,rating,timestamp)]
    val rating = ratingsRdd.map(_.split("::"))
    val usermovie = rating.map{ x =>
      (x(0), x(1))
    }.filter(_._2.equals(MOVIE_ID))

    //用户和电影的关联RDD，userRating
    val userRating = usermovie.join(users)

    //统计
    val userDistribution = userRating.map { x =>
      (x._2._2, 1)
    }.reduceByKey(_+_)

    //注意：这么打印是打印不出结果的。
    userDistribution.foreach(println)

    //使用collect或者take将结果返回到driver端才能打印，如上是打印到了各个Executor上
    userDistribution.collect().foreach(println)


  }
}
