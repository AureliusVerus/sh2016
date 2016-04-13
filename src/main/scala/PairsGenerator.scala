import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

case class PairScores(commonFriends: Int,
                      commonMainFriends: Int,
                      adamicAdar: Double,
                      adamicAdar2: Double,
                      pageRankScore: Double,
                      interactionScore: Double,
                      commonRelatives: Int,
                      commonColleagues: Int,
                      commonSchoolmates: Int,
                      commonArmyFellows: Int,
                      commonOthers: Int,
                      maskOr: Int,
                      maskAnd: Int)
case class Pair(uid1: Int, uid2: Int, scores: PairScores)

object PairsGenerator {
  val numPairsPartitions = 107

  def generatePairsFromUser(user: SocialGraphUser, numPartitions: Int, k: Int,
                            coreUsers: Broadcast[Set[Int]],
                            pageRanks: Broadcast[Map[Int, Double]],
                            friendsCount: Broadcast[Map[Int, Int]]) = {
    val pairs = ArrayBuffer.empty[((Int, Int), PairScores)]

    val friends = friendsCount.value.getOrElse(user.uid, 0)
    val mainFriend = if (coreUsers.value.contains(user.uid)) 1 else 0
    val adamicAdar = 1.0 / Math.log(friends.toDouble + 1.0)
    val adamicAdar2 = 1.0 / Math.log10(friends.toDouble + 1.0)
    val pageRankScore = pageRanks.value.getOrElse(user.uid, 0.0)

    val friendsForPairs = user.friends.filter(f => coreUsers.value.contains(f.uid))

    for (i <- 0 until friendsForPairs.length) {
      val p1 = friendsForPairs(i).uid

      if (p1 % numPartitions == k) {

        val mask1 = friendsForPairs(i).mask
        val isRelatives1 = MaskHelper.isRelatives(mask1)
        val isColleague1 = MaskHelper.isColleague(mask1)
        val isSchoolmate1 = MaskHelper.isSchoolmate(mask1)
        val isArmyFellow1 = MaskHelper.isArmyFellow(mask1)
        val isOther1 = MaskHelper.isOther(mask1)
        val interaction1 = friendsForPairs(i).interactionScore

        for (j <- i + 1 until friendsForPairs.length) {
          val p2 = friendsForPairs(j).uid

          val mask2 = friendsForPairs(j).mask
          val isRelatives2 = MaskHelper.isRelatives(mask2)
          val isColleague2 = MaskHelper.isColleague(mask2)
          val isSchoolmate2 = MaskHelper.isSchoolmate(mask2)
          val isArmyFellow2 = MaskHelper.isArmyFellow(mask2)
          val isOther2 = MaskHelper.isOther(mask2)
          val interaction2 = friendsForPairs(j).interactionScore

          val interactionScore = interaction1 * interaction2

          pairs.append(((p1, p2), PairScores(
            1, mainFriend, adamicAdar, adamicAdar2, pageRankScore, interactionScore,
            isRelatives1 * isRelatives2,
            isColleague1 * isColleague2,
            isSchoolmate1 * isSchoolmate2,
            isArmyFellow1 * isArmyFellow2,
            isOther1 * isOther2,
            mask1 | mask2,
            mask1 & mask2
          )))
        }
      }
    }

    pairs
  }

  def generate(reversedGraph: RDD[SocialGraphUser],
               part: Int,
               coreUsers: Broadcast[Set[Int]],
               pageRanks: Broadcast[Map[Int, Double]],
               friendsCount: Broadcast[Map[Int, Int]]) : RDD[Pair]= {

    reversedGraph
      .flatMap(t => generatePairsFromUser(t, numPairsPartitions, part, coreUsers, pageRanks, friendsCount))
      .reduceByKey((scores1, scores2) => {
        PairScores(
          scores1.commonFriends + scores2.commonFriends,
          scores1.commonMainFriends + scores2.commonMainFriends,
          scores1.adamicAdar + scores2.adamicAdar,
          scores1.adamicAdar2 + scores2.adamicAdar2,
          scores1.pageRankScore + scores2.pageRankScore,
          scores1.interactionScore + scores2.interactionScore,
          scores1.commonRelatives + scores2.commonRelatives,
          scores1.commonColleagues + scores2.commonColleagues,
          scores1.commonSchoolmates + scores2.commonSchoolmates,
          scores1.commonArmyFellows + scores2.commonArmyFellows,
          scores1.commonOthers + scores2.commonOthers,
          scores1.maskOr | scores2.maskOr,
          scores1.maskAnd | scores2.maskAnd
        )
      }).map(p => Pair(p._1._1, p._1._2, p._2))
      //.filter(p => p.scores.commonFriends > 1)
  }

  def readFromParquet(sqlc: SQLContext, path: String) : RDD[Pair] = {
    sqlc.read.parquet(path)
      .map{
        case Row(uid1: Int, uid2: Int, scores: Row) => Pair(uid1, uid2, PairScores(
          scores.getAs[Int](0), scores.getAs[Int](1),
          scores.getAs[Double](2), scores.getAs[Double](3),
          scores.getAs[Double](4), scores.getAs[Double](5),
          scores.getAs[Int](6), scores.getAs[Int](7),
          scores.getAs[Int](8), scores.getAs[Int](9),
          scores.getAs[Int](10),
          scores.getAs[Int](11), scores.getAs[Int](12)
        ))
      }
  }
}