package grasshopper.test.model

import geometry.Point
import grasshopper.test.util.Haversine

case class CheckGeocode(a: AddressPointGeocode, c: CensusGeocode)

case class AddressPointGeocode(inputAddress: InputAddress, p: Point, foundAddress: String, addressMatch: Double) {
  def dist: Double = {
    val p1 = inputAddress.point
    Haversine.distance(p1, p)
  }
}

case class CensusGeocode(inputAddress: InputAddress, p: Point) {
  def dist: Double = {
    val p1 = inputAddress.point
    Haversine.distance(p1, p)
  }
}

