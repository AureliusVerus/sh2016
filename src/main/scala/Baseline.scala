/**
  * Baseline for hackaton
  */


import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class PairWithCommonFriends(person1: Int, person2: Int, commonFriendsCount: Int)
case class UserFriends(user: Int, friends: Array[Int])
case class AgeSex(age: Int, sex: Int)

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

object Baseline {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Baseline")
      .set("spark.driver.maxResultSize", "2g")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val dataDir = if (args.length == 1) args(0) else "./"
    val paths = new Paths(dataDir, sc)

    val graph = {
      if (paths.exists(paths.graphWithInteractions)) {
        SocialGraph.readFromParquet(sqlc, paths.graphWithInteractions)
      } else {
        val sourceGraph = SocialGraph.loadFromText(sc, paths.graph)
        val interactionScores = Interactions.readFromParquet(sqlc, paths.interactions)
        val graphWithInteractions = SocialGraph.joinWithInteractions(sourceGraph, interactionScores)
        graphWithInteractions.toDF().write.parquet(paths.graphWithInteractions)

        graphWithInteractions
      }
    }

    if (!paths.exists(paths.reversedGraph)) {
      SocialGraph.reverse(graph).toDF().write.parquet(paths.reversedGraph)
    }

    val mainUsers = graph.map(user => user.uid)
    val mainUsersBC = sc.broadcast(mainUsers.collect().toSet)

    val pageRanks = {
      if (!paths.exists(paths.pageRank)) {
        PageRank.computeStatic(graph, mainUsersBC).toDF().write.parquet(paths.pageRank)
      }

      PageRank.readFromParquet(sqlc, paths.pageRank)
    }

    val maxPageRank = pageRanks.map(t => t._2).max()
    println("maxPageRank = " + maxPageRank)
    println("pageRanks count = " + pageRanks.count())
    println("mainUsers size = " + mainUsersBC.value.size)
    val pageRanksBC = sc.broadcast(pageRanks.collectAsMap())

    val mainUsersFriendsCount = graph.map(user => user.uid -> user.friends.length)
    val otherUsersFriendsCount = OtherDetails.readFriendsCount(sqlc, paths.otherDetails)
    val friendsCount = mainUsersFriendsCount.union(otherUsersFriendsCount)
    val friendsCountBC = sc.broadcast(friendsCount.collectAsMap())

    def generatePairsWithScores(user: SocialGraphUser, numPartitions: Int, k: Int) = {
      val pairs = ArrayBuffer.empty[((Int, Int), PairScores)]

      val friends = friendsCountBC.value.getOrElse(user.uid, 0)
      val mainFriend = if (mainUsersBC.value.contains(user.uid)) 1 else 0
      val adamicAdar = 1.0 / Math.log(friends.toDouble + 1.0)
      val adamicAdar2 = 1.0 / Math.log10(friends.toDouble + 1.0)
      val pageRankScore = pageRanksBC.value.getOrElse(user.uid, 0.0)

      for (i <- 0 until user.friends.length) {
        val p1 = user.friends(i).uid

        if (p1 % numPartitions == k && mainUsersBC.value.contains(p1)) {

          val mask1 = user.friends(i).mask
          val isRelatives1 = MaskHelper.isRelatives(mask1)
          val isColleague1 = MaskHelper.isColleague(mask1)
          val isSchoolmate1 = MaskHelper.isSchoolmate(mask1)
          val isArmyFellow1 = MaskHelper.isArmyFellow(mask1)
          val isOther1 = MaskHelper.isOther(mask1)
          val interaction1 = user.friends(i).interactionScore

          for (j <- 0 until user.friends.length) {
            if (i != j) {
              val p2 = user.friends(j).uid

              if (mainUsersBC.value.contains(p2)) {
                val mask2 = user.friends(j).mask
                val isRelatives2 = MaskHelper.isRelatives(mask2)
                val isColleague2 = MaskHelper.isColleague(mask2)
                val isSchoolmate2 = MaskHelper.isSchoolmate(mask2)
                val isArmyFellow2 = MaskHelper.isArmyFellow(mask2)
                val isOther2 = MaskHelper.isOther(mask2)
                val interaction2 = user.friends(j).interactionScore

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
        }
      }

      pairs
    }

    for (k <- 0 until SocialGraph.numPairsPartitions) {
      if (!paths.exists(paths.getPairsPart(k))) {
        val commonFriendPairs = {
          SocialGraph.readFromParquet(sqlc, paths.reversedGraph)
            .flatMap(t => generatePairsWithScores(t, SocialGraph.numPairsPartitions, k))
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

        commonFriendPairs.repartition(16).toDF.write.parquet(paths.getPairsPart(k))
      }
    }

  }
}
