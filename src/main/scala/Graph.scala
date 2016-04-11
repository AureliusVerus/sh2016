import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


object Graph {
  case class UserMask(user:Int, mask:Int)
  case class UserFriends(user: Int, friends: Array[UserMask])

  def loadFromCSV(sc: SparkContext, path: String) = {
    val graph = {
      sc.textFile(path)
        .map(line => {
          val lineSplit = line.split("\t")
          val user = lineSplit(0).toInt
          val rawFriends = {
            lineSplit(1)
              .replace("{(", "")
              .replace(")}", "")
              .split("\\),\\(")
          }
          val friends = rawFriends.map(t => UserMask(t.split(",")(0).toInt, t.split(",")(1).toInt))
          UserFriends(user, friends)
        })
    }

    graph
  }

  def reverse(graph: RDD[UserFriends], sqlc: SQLContext, path: String, partitions: Int = 200) = {
    import sqlc.implicits._

    graph
      .filter(userFriends => userFriends.friends.length >= 8 && userFriends.friends.length <= 1000)
      .flatMap(userFriends => userFriends.friends.map(x => (x.user, UserMask(userFriends.user, x.mask))))
      .groupByKey(partitions)
      .map(t => UserFriends(t._1, t._2.toArray.sortWith(_.user < _.user)))
      .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= 2000)
      .map(friends => new Tuple1(friends))
      .toDF
      .write.parquet(path)
  }
}