package grasshopper.test.model

case class TestGeocode(inputAddress: String, x: Double, y: Double, tract: String, ax: Double, ay: Double, aFoundAddress: String, addressMatch: Double, cx: Double, cy: Double, cFoundAddress: String)

object TestGeocode {
  def apply(inputAddress: String, x: Double, y: Double): TestGeocode =
    TestGeocode(inputAddress, x, y, "", 0, 0, "", 0.0, 0, 0, "")
  def apply(inputAddress: String, x: Double, y: Double, tract: String): TestGeocode =
    TestGeocode(inputAddress, x, y, tract, 0, 0, "", 0.0, 0, 0, "")
  def apply(inputAddress: String, x: Double, y: Double, tract: String, ax: Double, ay: Double, aFoundAddress: String, addressMatch: Double): TestGeocode =
    TestGeocode(inputAddress, x, y, tract, ax, ay, aFoundAddress, addressMatch, 0, 0, "")
}