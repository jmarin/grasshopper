package grasshopper.test.util

import org.scalatest.{ MustMatchers, FlatSpec }

class HaversineSpec extends FlatSpec with MustMatchers {

  "The Haversine formula" should "calculate the distance between two coordinates" in {
    val lat1 = 36.12
    val lon1 = -86.67
    val lat2 = 33.94
    val lon2 = -118.40

    Haversine.distance(lat1, lon1, lat2, lon2) mustBe 2887.2599506071106
  }
}
