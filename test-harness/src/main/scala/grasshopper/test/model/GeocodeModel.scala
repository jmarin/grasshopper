package grasshopper.test.model

import geometry.Point

object GeocodeModel {

  object PointInputAddress {
    def empty: PointInputAddress = PointInputAddress("", Point(0, 0))
  }

  case class PointInputAddress(inputAddress: String, point: Point)

  object PointInputAddressTract {
    def empty: PointInputAddressTract = PointInputAddressTract(PointInputAddress.empty, "")
  }

  case class PointInputAddressTract(pointInputAddress: PointInputAddress, geoid: String) {
    override def toString: String = s"${pointInputAddress.inputAddress},${pointInputAddress.point.x},${pointInputAddress.point.y},${geoid}"
  }

  object AddressPointGeocode {
    def empty: AddressPointGeocode = AddressPointGeocode(PointInputAddress.empty, Point(0, 0), "", 0.0, 0.0)
  }

  case class AddressPointGeocode(inputAddress: PointInputAddress, point: Point, foundAddress: String, addressMatch: Double, distance: Double) {
    override def toString(): String = {
      s"${inputAddress.inputAddress}," +
        s"${inputAddress.point.x}," +
        s"${inputAddress.point.y}," +
        s" $foundAddress," +
        s" ${point.x}," +
        s" ${point.y}," +
        s" $addressMatch," +
        s" $distance"
    }
  }

  object AddressPointGeocodeTract {
    def empty: AddressPointGeocodeTract = AddressPointGeocodeTract(AddressPointGeocode.empty, "00000000000")
  }

  case class AddressPointGeocodeTract(addressPointGeocode: AddressPointGeocode, geoid: String) {
    override def toString(): String = {
      s"${addressPointGeocode.inputAddress}," +
        s"${addressPointGeocode.inputAddress.point.x}," +
        s"${addressPointGeocode.inputAddress.point.y}," +
        s" ${addressPointGeocode.foundAddress}," +
        s" ${addressPointGeocode.point.x}," +
        s" ${addressPointGeocode.point.y}," +
        s" ${addressPointGeocode.addressMatch}," +
        s" ${addressPointGeocode.distance}," +
        s" $geoid"
    }
  }

  case class CensusInputAddress(number: Int, streetName: String, zipCode: String, state: String, point: Point) {
    override def toString(): String = {
      s"${number} ${streetName} ${state} ${zipCode}"
    }
    def isEmpty: Boolean = this.number == 0 && this.streetName == "" && this.zipCode == "" && this.state == ""
  }

  object CensusInputAddress {
    def empty: CensusInputAddress = CensusInputAddress(0, "", "", "", Point(0, 0))
  }

  object PointInputAddressTractParsed {
    def empty: PointInputAddressTractParsed = PointInputAddressTractParsed(PointInputAddressTract.empty, CensusInputAddress.empty)
  }

  case class PointInputAddressTractParsed(addressPointGeocodeTract: PointInputAddressTract, censusInputAddress: CensusInputAddress) {
    override def toString(): String = {
      s"${addressPointGeocodeTract.pointInputAddress.inputAddress}," +
        s"${addressPointGeocodeTract.pointInputAddress.point.x}," +
        s"${addressPointGeocodeTract.pointInputAddress.point.y}," +
        s"${censusInputAddress.toString()}"
    }
  }

  case class CensusGeocodePoint(pointTract: PointInputAddressTract, point: Point, distance: Double) {
    override def toString(): String = {
      s"${pointTract.pointInputAddress.inputAddress}," +
        s"${pointTract.pointInputAddress.point.x}," +
        s"${pointTract.pointInputAddress.point.y}," +
        s"${pointTract.geoid}" +
        s"${point.x}," +
        s"${point.y}," +
        s"$distance"
    }
  }

  case class CensusGeocodeTract(censusPointGeocode: CensusGeocodePoint, geoid: String) {
    override def toString(): String = {
      s"${censusPointGeocode.pointTract.pointInputAddress.inputAddress}," +
        s"${censusPointGeocode.pointTract.pointInputAddress.point.x}," +
        s"${censusPointGeocode.pointTract.pointInputAddress.point.y}," +
        s"${censusPointGeocode.pointTract.geoid}," +
        s"${censusPointGeocode.point.x}," +
        s"${censusPointGeocode.point.y}," +
        s"${censusPointGeocode.distance}," +
        s"${geoid}"
    }

    def toCSV: String = {
      toString() + "\n"
    }
  }

  case class CensusGeocodeResult(inputPoint: PointInputAddressTract, geocodedPoint: CensusGeocodeTract) {
    def toCSV(): String = {
      s"${inputPoint.pointInputAddress.inputAddress},${inputPoint.pointInputAddress.point.x},${inputPoint.pointInputAddress.point.y},${inputPoint.geoid},${geocodedPoint.censusPointGeocode.point.x},${geocodedPoint.censusPointGeocode.point.y},${geocodedPoint.censusPointGeocode.distance},${geocodedPoint.geoid}\n"
    }
  }

  case class TestResult(inputAddressTract: PointInputAddressTract, addressPointTract: AddressPointGeocodeTract, censusGeocodeTract: CensusGeocodeTract)

}

