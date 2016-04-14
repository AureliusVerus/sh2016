import breeze.numerics.abs
import org.joda.time.DateTime

object Utils {
  val ageStartDate = new DateTime(1970, 1, 1, 0, 0)

  def getUserAge(days:Int):Double = {
    val meanAge = 33.0
    val medianAge = 29.0

    if (days == 0) {
      meanAge
    }
    else {
      val d = if (days >= 0) ageStartDate.plusDays(days) else ageStartDate.minusDays(abs(days))
      val age = 2016 - d.getYear
      age.toDouble
    }
  }

  def getUsersAgeDiff(bd1:Int, bd2:Int):Double = {
    abs(getUserAge(bd1) - getUserAge(bd2))
  }
}