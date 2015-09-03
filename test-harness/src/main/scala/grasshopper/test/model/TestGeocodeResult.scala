package grasshopper.test.model

case class TestGeocodeResult(inputAddress: String, x: Double, y: Double, tract: String, ax: Double, ay: Double, aFoundAddress: String, cx: Double, cy: Double, cFoundAddress: String)

object TestGeocodeResult {
  def apply(inputAddress: String, x: Double, y: Double): TestGeocodeResult =
    TestGeocodeResult(inputAddress, x, y, "", 0, 0, "", 0, 0, "")
  def apply(inputAddress: String, x: Double, y: Double, tract: String): TestGeocodeResult =
    TestGeocodeResult(inputAddress, x, y, tract, 0, 0, "", 0, 0, "")
  def apply(inputAddress: String, x: Double, y: Double, tract: String, ax: Double, ay: Double, aFoundAddress: String): TestGeocodeResult =
    TestGeocodeResult(inputAddress, x, y, tract, ax, ay, aFoundAddress, 0, 0, "")
}