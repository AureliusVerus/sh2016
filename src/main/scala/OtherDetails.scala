import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

object OtherDetails {

  def readFriendsCount(sqlc: SQLContext, path: String) : RDD[(Int, Int)] = {
    sqlc.read.parquet(path)
      .map(r => (r.getAs[Long](0).toInt, r.getAs[Long](8).toInt))
  }
}