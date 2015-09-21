package grasshopper.test.model

import geometry.Point

case class AddressPointGeocode(inputAddress: InputAddress, p: Point, foundAddress: String, addressMatch: Double, distance: Double)

case class CensusGeocode(inputAddress: InputAddress, p: Point, distance: Double)

