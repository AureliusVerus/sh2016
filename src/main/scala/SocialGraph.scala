import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

case class SocialGraphFriend(uid: Int, mask: Int, interactionScore: Double = 0f)
case class SocialGraphUser(uid: Int, friends: Array[SocialGraphFriend])

object SocialGraph {
  val numGraphPartitions = 200

  def loadFromText(sc: SparkContext, path: String) = {
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
          val friends = rawFriends.map(t => SocialGraphFriend(t.split(",")(0).toInt, t.split(",")(1).toInt))
          SocialGraphUser(user, friends)
        })
    }

    graph
  }

  def readFromParquet(sqlc: SQLContext, path: String) : RDD[SocialGraphUser] = {
    sqlc.read.parquet(path).map((r: Row) => {
      val user = r.getAs[Int](0)
      val friends = r.getAs[Seq[Row]](1).map { case Row(uid: Int, mask: Int, score: Double) => SocialGraphFriend(uid, mask, score) }.toArray
      SocialGraphUser(user, friends)
    })
  }

  def joinWithInteractions(graph: RDD[SocialGraphUser], interactions: RDD[((Int, Int), Double)]) : RDD[SocialGraphUser] = {
    graph.flatMap(user => user.friends.map(x => ((user.uid, x.uid), x.mask)))
      .leftOuterJoin(interactions)
      .map(x => {
        val userId = x._1._1
        val friendId = x._1._2
        val mask = x._2._1
        val score = x._2._2.getOrElse(0.0)

        userId -> SocialGraphFriend(friendId, mask, score)
      })
      .groupByKey(numGraphPartitions)
      .map(x => SocialGraphUser(x._1, x._2.toArray))
  }

  def reverse(graph: RDD[SocialGraphUser], filterMinFriends: Int = 8, filterMaxFriends: Int = 1000) : RDD[SocialGraphUser] = {
    graph
      .filter(user => user.friends.length >= filterMinFriends && user.friends.length <= filterMaxFriends)
      .flatMap(user => user.friends.map(x => (x.uid, SocialGraphFriend(user.uid, x.mask, x.interactionScore))))
      .groupByKey(numGraphPartitions)
      .map(t => SocialGraphUser(t._1, t._2.toArray.sortWith(_.uid < _.uid)))
      .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= 2000)
  }
}