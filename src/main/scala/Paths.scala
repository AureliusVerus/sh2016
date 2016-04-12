import org.apache.hadoop.fs._
import org.apache.spark.SparkContext

class Paths(val base: String, val context: SparkContext) {
  private val fs = FileSystem.get(context.hadoopConfiguration)

  val graph = base + "trainGraph"
  val demography = base + "demography"
  val otherDetails = base + "otherDetails"
  val interactions = base + "interactions"

  val graphWithInteractions = base + "graphWithInteractions"
  val pageRank = base + "userPageRank"
  val reversedGraph = base + "reversedGraph"
  val pairs = base + "pairs"

  val prediction = base + "prediction"
  val model = base + "LogisticRegressionModel"

  def getPairsPart(n: Int) = pairs + "/part_" + n

  def exists(path: String) = fs.exists(new Path(path))
}