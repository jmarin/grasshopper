package grasshopper.test.model

import geometry.Point

case class AddressPointGeocode(inputAddress: PointInputAddress, point: Point, foundAddress: String, addressMatch: Double, distance: Double) {
  override def toString(): String = {
    s"${inputAddress.inputAddress}" +
      s"${inputAddress.point.y}" +
      s"${inputAddress.point.x}" +
      s" $foundAddress," +
      s" ${point.y}," +
      s" ${point.x}," +
      s" $addressMatch," +
      s" $distance"
  }
}

case class AddressPointGeocodeTract(addressPointGeocode: AddressPointGeocode, geoid10: String) {
  override def toString(): String = {
    s"${addressPointGeocode.inputAddress}," +
      s"${addressPointGeocode.inputAddress.point.y}" +
      s"${addressPointGeocode.inputAddress.point.x}" +
      s" ${addressPointGeocode.foundAddress}," +
      s" ${addressPointGeocode.point.y}," +
      s" ${addressPointGeocode.point.x}," +
      s" ${addressPointGeocode.addressMatch}," +
      s" ${addressPointGeocode.distance}," +
      s" $geoid10"
  }
}

case class CensusGeocode(inputAddress: PointInputAddress, point: Point, distance: Double) {
  override def toString(): String = {
    s"${inputAddress.inputAddress}," +
      s"${inputAddress.point.y}," +
      s"${inputAddress.point.x}," +
      s"${point.y}," +
      s"${point.x}," +
      s"$distance"
  }
}

