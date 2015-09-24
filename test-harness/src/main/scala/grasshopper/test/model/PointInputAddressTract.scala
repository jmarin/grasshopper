package grasshopper.test.model

import geometry.Point

object PointInputAddressTract {
  def empty: PointInputAddressTract = PointInputAddressTract(PointInputAddress.empty, "")
}

case class PointInputAddressTract(pointInputAddress: PointInputAddress, geoid: String)
