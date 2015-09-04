package grasshopper.test

import akka.stream.scaladsl.Flow
import feature._
import geometry.Point
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.test.model.TestGeocodeResult

import scala.concurrent.ExecutionContext

object GeocodeETL {

  def address2GeocodeTestResult: Flow[String, TestGeocodeResult, Unit] = {
    Flow[String]
      .map(a => parseAddress(a, ","))
  }

  def overlayTract: Flow[TestGeocodeResult, TestGeocodeResult, Unit] = {
    Flow[TestGeocodeResult]
      .map(t => tractJoin(t))
  }

  def addressPointsGeocode(implicit ec: ExecutionContext): Flow[TestGeocodeResult, TestGeocodeResult, Unit] = {
    Flow[TestGeocodeResult]
      .mapAsync(4) { t =>
        val a = t.inputAddress
        val lon = t.x
        val lat = t.y
        val tract = t.tract
        for {
          x <- AddressPointsClient.geocode(a) if x.isRight
          result = x.right.getOrElse(AddressPointsResult.empty)
          features = result.features.toList
          longitude = if (features.nonEmpty) features.head.geometry.centroid.x else 0
          latitude = if (features.nonEmpty) features.head.geometry.centroid.y else 0
          foundAddress = if (features.nonEmpty) features.head.get("address").getOrElse("").toString else ""
        } yield TestGeocodeResult(a, lon, lat, tract, lon, lat, foundAddress, 1)
      }
  }

  def addressPointsFlow(implicit ec: ExecutionContext): Flow[Feature, Feature, Unit] = {
    Flow[Feature]
      .mapAsync(4) { f =>
        val a = f.get("address").getOrElse("")
        val g = f.geometry
        val tract = f.get("GEOID10").getOrElse("")
        AddressPointsClient.geocode(a.toString).map { x =>
          if (x.isRight) {
            val result = x.right.getOrElse(AddressPointsResult.empty)
            if (result.features.size > 0) {
              result.features(0)
            } else {
              Feature(Point(0, 0))
            }
          } else {
            Feature(Point(0, 0))
          }
        }
      }
  }

  def toCSV: Flow[TestGeocodeResult, String, Unit] = {
    Flow[TestGeocodeResult]
      .map { t =>
        println(t)
        s"${t.inputAddress},${t.x},${t.y},${t.tract},${t.ax},${t.ay},${t.aFoundAddress},${t.addressMatch},${t.cx},${t.cy}"
      }
  }

  //  def toCsv: Flow[Feature, String, Unit] = {
  //    Flow[Feature]
  //      .map { f =>
  //        val p = f.geometry.asInstanceOf[Point]
  //        val x = p.x
  //        val y = p.y
  //        val address = f.get("address").getOrElse("")
  //        val tract = f.get("GEOID10").getOrElse("")
  //        s"${address},${x},${y},${tract}"
  //      }
  //  }

  //Dummy function for now. Replace with real point in poly lookup
  def tractJoin(t: TestGeocodeResult): TestGeocodeResult = {
    val tract = "01234567890"
    TestGeocodeResult(t.inputAddress, t.x, t.y, tract)
  }

  private def parseAddress1(address: String, separator: String): Feature = {
    val parts = address.split(separator)
    val addr = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    val point = Point(x, y)
    val schema = Schema(List(
      Field("geometry", GeometryType()),
      Field("address", StringType())
    ))
    val values = Map("geometry" -> point, "address" -> addr)
    Feature(schema, values)
  }

  private def parseAddress(address: String, separator: String): TestGeocodeResult = {
    val parts = address.split(separator)
    val addr = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    TestGeocodeResult(addr, x, y)
  }

}
