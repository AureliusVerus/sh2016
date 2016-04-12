import org.apache.spark.sql.{Row, SQLContext}

object Interactions {

  def getInteractionScore(interactionEntries: Seq[(Int, Double)]): Double = interactionEntries.map(x => x._2).sum

  def readFromParquet(sqlc: SQLContext, path: String) = {
    sqlc.read.parquet(path)
      .map((r: Row) => (r.getAs[Long](0), r.getAs[Long](1), r.getAs[Seq[Row]](2).map{
        case Row(index: Int, value: Double) => (index, value)
      }))
      .map(x => (x._1.toInt, x._2.toInt) -> getInteractionScore(x._3))
      .repartition(129)
  }
}