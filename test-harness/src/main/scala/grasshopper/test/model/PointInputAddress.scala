package grasshopper.test.model

import geometry.Point

object PointInputAddress {
  def empty: PointInputAddress = PointInputAddress("", Point(0, 0))
}

case class PointInputAddress(inputAddress: String, point: Point)
