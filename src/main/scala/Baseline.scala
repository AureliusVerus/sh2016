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

    for (k <- 0 until PairsGenerator.numPairsPartitions) {
      if (!paths.exists(paths.getPairsPart(k))) {
        val reversedGraph = SocialGraph.readFromParquet(sqlc, paths.reversedGraph)
        val pairsPart = PairsGenerator.generate(reversedGraph, k, mainUsersBC, pageRanksBC, friendsCountBC)

        pairsPart.repartition(16).toDF.write.parquet(paths.getPairsPart(k))
      }
    }

  }
}
