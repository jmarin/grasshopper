package grasshopper.test

import akka.stream.scaladsl.Flow
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.test.model.TestGeocodeResult

import scala.concurrent.ExecutionContext

object GeocodeETL {

  def addressParse: Flow[String, TestGeocodeResult, Unit] = {
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

  def toCSV: Flow[TestGeocodeResult, String, Unit] = {
    Flow[TestGeocodeResult]
      .map { t =>
        println(t)
        s"${t.inputAddress},${t.x},${t.y},${t.tract},${t.ax},${t.ay},${t.aFoundAddress},${t.addressMatch},${t.cx},${t.cy}"
      }
  }

  //Dummy function for now. Replace with real point in poly lookup
  def tractJoin(t: TestGeocodeResult): TestGeocodeResult = {
    val tract = "01234567890"
    TestGeocodeResult(t.inputAddress, t.x, t.y, tract)
  }

  private def parseAddress(address: String, separator: String): TestGeocodeResult = {
    val parts = address.split(separator)
    val addr = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    TestGeocodeResult(addr, x, y)
  }

}
