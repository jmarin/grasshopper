package grasshopper.test.model

case class CensusGeocodeResult(inputPoint: PointInputAddressTract, geocodedPoint: CensusGeocodeTract) {
  def toCSV(): String = {
    s"${inputPoint.pointInputAddress.inputAddress},${inputPoint.pointInputAddress.point.x},${inputPoint.pointInputAddress.point.y},${inputPoint.geoid},${geocodedPoint.censusPointGeocode.point.x},${geocodedPoint.censusPointGeocode.point.y},${geocodedPoint.censusPointGeocode.distance},${geocodedPoint.geoid}\n"
  }
}
