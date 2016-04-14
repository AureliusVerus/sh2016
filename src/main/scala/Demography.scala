import org.apache.spark.SparkContext

case class DemographyData(createDate:Long, birthDate: Int, sex: Int, countryId: Long, locationId: Long, loginRegion: Long)

object Demography {

  def loadFromText(sc: SparkContext, path: String) = {
    sc.textFile(path)
      .map(line => {
        val lineSplit = line.trim().split("\t")
        val userId = lineSplit(0).toInt
        val createDate = lineSplit(1).toLong
        val birthDate = if (lineSplit(2) == "") 0 else lineSplit(2).toInt
        val sex = lineSplit(3).toInt
        val countryId = lineSplit(4).toLong
        val locationId = lineSplit(5).toLong
        val loginRegion = if (lineSplit.length == 7) lineSplit(6).toLong else 0

        userId -> DemographyData(createDate, birthDate, sex, countryId, locationId, loginRegion)
      })
  }
}