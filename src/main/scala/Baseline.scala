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

    val demography = Demography.loadFromText(sc, paths.demography)
    val countriesCount = demography.map(x => x._2.countryId -> 1).reduceByKey(_+_)
    val locationsCount = demography.map(x => x._2.locationId -> 1).reduceByKey(_+_)
    val loginRegionsCount = demography.map(x => x._2.loginRegion -> 1).reduceByKey(_+_)

    val demographyBC = sc.broadcast(demography.collectAsMap())
    val countriesCountBC = sc.broadcast(countriesCount.collectAsMap())
    val locationsCountBC = sc.broadcast(locationsCount.collectAsMap())
    val loginRegionsCountBC = sc.broadcast(loginRegionsCount.collectAsMap())

    val positives = graph.flatMap(user =>
      user.friends
        .filter(f => mainUsersBC.value.contains(f.uid) && f.uid > user.uid)
        .map(f => (user.uid, f.uid) -> 1.0)
    )

    def getJaccardSimilarity(commonFriends: Int, friendsCount1:Int, friendsCount2:Int) : Double = {
      val union = friendsCount1 + friendsCount2 - commonFriends

      if (union == 0) {
        0.0
      } else {
        commonFriends.toDouble / union.toDouble
      }
    }

    def getCosineSimilarity(commonFriends: Int, friendsCount1:Int, friendsCount2:Int) : Double = {
      if (friendsCount1 == 0 && friendsCount2 == 0) {
        0.0
      } else {
        commonFriends.toDouble / math.sqrt(friendsCount1 * friendsCount2)
      }
    }

    def getFeatures(pair: Pair) = {
      val features = ArrayBuffer.empty[Double]

      val friendsCount1 = friendsCountBC.value.getOrElse(pair.uid1, 0)
      val friendsCount2 = friendsCountBC.value.getOrElse(pair.uid2, 0)
      val demography1 = demographyBC.value.getOrElse(pair.uid1, DemographyData(0, 0, 0, 0, 0, 0))
      val demography2 = demographyBC.value.getOrElse(pair.uid2, DemographyData(0, 0, 0, 0, 0, 0))
      val pr1 = pageRanksBC.value.getOrElse(pair.uid1, 0.0)
      val pr2 = pageRanksBC.value.getOrElse(pair.uid2, 0.0)
      val prn1 = pr1 / maxPageRank
      val prn2 = pr2 / maxPageRank
      val countrySize1 = countriesCountBC.value.getOrElse(demography1.countryId, 0)
      val countrySize2 = countriesCountBC.value.getOrElse(demography2.countryId, 0)
      val locationSize1 = locationsCountBC.value.getOrElse(demography1.locationId, 0)
      val locationSize2 = locationsCountBC.value.getOrElse(demography2.locationId, 0)
      val loginSize1 = loginRegionsCountBC.value.getOrElse(demography1.locationId, 0)
      val loginSize2 = loginRegionsCountBC.value.getOrElse(demography2.locationId, 0)
      val minFriendsCount = math.min(friendsCount1, friendsCount2)

      val jaccard = getJaccardSimilarity(pair.scores.commonFriends, friendsCount1, friendsCount2)
      val cosine = getCosineSimilarity(pair.scores.commonFriends, friendsCount1, friendsCount2)
      val normalizedCommonFriends = if (minFriendsCount == 0) 0.0 else pair.scores.commonFriends.toDouble / minFriendsCount.toDouble

      val pageRankScoreMean = if (pair.scores.commonMainFriends == 0) 0.0 else pair.scores.pageRankScore / pair.scores.commonMainFriends.toDouble
      val pageRankScoreMeanNormalized = pageRankScoreMean / maxPageRank
      val pageRankDiff = abs(pr1 - pr2)
      val pageRankDiffNormalized = abs(prn1 - prn2)
      val pageRankMean = (pr1 + pr2) * 0.5
      val pageRankMeanNormalized = (prn1 + prn2) * 0.5

      val ageDiff = Utils.getUsersAgeDiff(demography1.birthDate, demography2.birthDate)
      val sameSex = if (demography1.sex == demography2.sex) 1.0 else 0.0
      val sameCountry = if (demography1.countryId == demography2.countryId) 1.0 else 0.0
      val sameLocation = if (demography1.locationId == demography2.locationId) 1.0 else 0.0
      val sameLoginRegion = if (demography1.loginRegion == demography2.loginRegion) 1.0 else 0.0

      features.append(ageDiff)
      features.append(if (ageDiff <= 2) 1.0 else 0.0)
      features.append(if (ageDiff > 2 && ageDiff <= 5) 1.0 else 0.0)
      features.append(if (ageDiff > 5 && ageDiff <= 8) 1.0 else 0.0)
      features.append(if (ageDiff > 8 && ageDiff <= 10) 1.0 else 0.0)
      features.append(if (ageDiff > 10) 1.0 else 0.0)

      features.append(sameSex)
      features.append(sameCountry)
      features.append(sameLocation)
      features.append(sameLoginRegion)

      features.append(abs(countrySize1 - countrySize2))
      features.append(abs(locationSize1 - locationSize2))

      features.append(jaccard)
      features.append(cosine)
      features.append(normalizedCommonFriends)
      features.append(pair.scores.adamicAdar)
      features.append(Math.log(pair.scores.adamicAdar + 1.0))
      features.append(pair.scores.adamicAdar2)
      features.append(Math.log(pair.scores.adamicAdar2 + 1.0))
      features.append(pair.scores.commonFriends)
      features.append(Math.log(pair.scores.commonFriends + 1.0))
      features.append(pair.scores.commonMainFriends)
      features.append(Math.log(pair.scores.commonMainFriends + 1.0))
      features.append(pair.scores.pageRankScore)
      features.append(Math.log(pair.scores.pageRankScore + 1.0))
      features.append(pair.scores.interactionScore)
      //features.append(Math.log(pair.scores.interactionScore + 1.0))
      features.append(pair.scores.commonRelatives)
      features.append(pair.scores.commonColleagues)
      features.append(pair.scores.commonSchoolmates)
      features.append(pair.scores.commonArmyFellows)
      features.append(pair.scores.commonOthers)
      features.append(if (pair.scores.maskOr > 1) 1.0 else 0.0)
      features.append(if (pair.scores.maskAnd > 1) 1.0 else 0.0)

      features.append(abs(friendsCount1 - friendsCount2))

      features.append(Math.log((friendsCount1 * friendsCount2) + 1.0))

      features.append(pageRankScoreMean)
      features.append(pageRankScoreMeanNormalized)
      features.append(pageRankDiff)
      features.append(pageRankDiffNormalized)
      features.append(pageRankMean)
      features.append(pageRankMeanNormalized)

     /* val nans = ArrayBuffer.empty[Double]
      for (k <- 0 until features.length) {
        if (features(k).isNaN)
          nans.append(k)
      }*/

      Vectors.dense(features.toArray)
    }

    def prepareTrainData(pairs: RDD[Pair], positives: RDD[((Int, Int), Double)]) = {
      val data = {
        pairs.map(p => (p.uid1, p.uid2) -> p)
          .leftOuterJoin(positives)
          .filter(p => p._1._1 % 11 != 7 && p._1._2 % 11 != 7)
          .map(p => LabeledPoint(p._2._2.getOrElse(0.0), getFeatures(p._2._1)))
      }

      val positiveData = data.filter(x => x.label == 1.0)
      val negativeData = data.filter(x => x.label == 0.0)

      val positiveCount = positiveData.count()
      val negativeCount = negativeData.count()

      val labledSetSize = math.min(positiveCount, negativeCount) - 1

      val positiveDataClamped = sc.parallelize(positiveData.take(labledSetSize.toInt))
      val negativeDataClamped = sc.parallelize(negativeData.take(labledSetSize.toInt))

      val positiveSplits = positiveDataClamped.randomSplit(Array(0.7, 0.3))
      val negativeSplits = negativeDataClamped.randomSplit(Array(0.7, 0.3))

      val training = positiveSplits(0).union(negativeSplits(0))
      val validation = positiveSplits(1).union(negativeSplits(1))

      (training, validation)
    }

    def prepareTestData(pairs: RDD[Pair], positives: RDD[((Int, Int), Double)]) = {
      val data = {
        pairs.map(p => (p.uid1, p.uid2) -> p)
          .leftOuterJoin(positives)
          .filter(p => p._1._1 % 11 == 7 || p._1._2 % 11 == 7)
          .map(p => p._1 -> LabeledPoint(p._2._2.getOrElse(0.0), getFeatures(p._2._1)))
          .filter(t => t._2.label == 0.0)
      }

      data
    }

    val trainData = prepareTrainData(PairsGenerator.readFromParquet(sqlc, paths.getPairsPart(33)), positives)
    val training = trainData._1
    val validation = trainData._2

    val model = {
      val logit = new LogisticRegressionWithLBFGS()
      logit.setNumClasses(2).optimizer.setRegParam(0.01)
      logit.run(training)
    }

    model.clearThreshold()
    model.save(sc, paths.model)

    val trainPredictions = validation.map(lp => (model.predict(lp.features), lp.label))
    @transient val metrics = new BinaryClassificationMetrics(trainPredictions, 100)
    val threshold = metrics.recallByThreshold().sortBy(-_._2).take(1)(0)._1

    println("AUC ROC = " + metrics.areaUnderROC())

    val testPredictions = {
      prepareTestData(PairsGenerator.readFromParquet(sqlc, paths.pairs + "/part_*/"), positives)
        .flatMap { case (id, LabeledPoint(label, features)) =>
          val prediction = model.predict(features)
          Seq(id._1 -> (id._2, prediction), id._2 -> (id._1, prediction))
        }
        .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
        .groupByKey(200)
        .map(t => {
          val user = t._1
          val firendsWithRatings = t._2
          val topBestFriends = firendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
          (user, topBestFriends)
        })
        .sortByKey(true, 1)
        .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPredictions.saveAsTextFile(paths.prediction,  classOf[GzipCodec])
  }
}
