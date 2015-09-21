package grasshopper.test.model

import geometry.Point

case class CensusInputAddress(number: Int, streetName: String, zipCode: String, state: String, point: Point) {
  override def toString(): String = {
    s"${number} ${streetName} ${state} ${zipCode}"
  }
  def isEmpty: Boolean = this.number == 0 && this.streetName == "" && this.zipCode == "" && this.state == ""
}

object CensusInputAddress {
  def empty: CensusInputAddress = CensusInputAddress(0, "", "", "", Point(0, 0))
}

