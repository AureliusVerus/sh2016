import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SQLContext}

case class UserPageRank(user: Int, rank: Double)

object PageRank {

  def computeStatic(graph: RDD[SocialGraphUser], coreUsers: Broadcast[Set[Int]], numIterations: Int = 3) : RDD[UserPageRank] = {
    val edges = {
      graph.flatMap(user =>
        user.friends
          .filter(f => coreUsers.value.contains(f.uid))
          .map(x => Edge(user.uid: VertexId, x.uid: VertexId, 1))
      )
    }

    Graph.fromEdges(edges, 1).staticPageRank(numIterations).vertices.map(v => UserPageRank(v._1.toInt, v._2))
  }

  def readFromParquet(sqlc: SQLContext, path: String)  = {
    sqlc.read.parquet(path)
      .map{ case Row(k: Int, v: Double) => k.toInt -> v }
  }
}