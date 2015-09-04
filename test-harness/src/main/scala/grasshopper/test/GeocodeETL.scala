package grasshopper.test

import akka.stream.scaladsl.{ Flow, FlowGraph }
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.client.census.model.ParsedInputAddress
import grasshopper.client.parser.AddressParserClient
import grasshopper.client.parser.model.ParsedAddress
import grasshopper.test.model.TestGeocode

import scala.concurrent.ExecutionContext

object GeocodeETL {

  def addressRead: Flow[String, TestGeocode, Unit] = {
    Flow[String]
      .map(a => readAddress(a, ","))
  }

  def overlayTract: Flow[TestGeocode, TestGeocode, Unit] = {
    Flow[TestGeocode]
      .map(t => tractJoin(t))
  }

  def addressPointsGeocode(implicit ec: ExecutionContext): Flow[TestGeocode, TestGeocode, Unit] = {
    Flow[TestGeocode]
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
        } yield TestGeocode(a, lon, lat, tract, lon, lat, foundAddress, 1)
      }
  }

  def addressParse(implicit ec: ExecutionContext): Flow[TestGeocode, ParsedInputAddress, Unit] = {
    Flow[TestGeocode]
      .mapAsync(4) { a =>
        for {
          x <- AddressParserClient.standardize(a.inputAddress)
          y = x.right.getOrElse(ParsedAddress.empty)
          p = ParsedInputAddress(y.parts.addressNumber.toInt, y.parts.streetName, y.parts.zip, y.parts.state)
        } yield p
      }
  }

  //  def censusGeocode(implicit ec: ExecutionContext): Flow[TestGeocodeResult, TestGeocodeResult, Unit] = {
  //    Flow[TestGeocodeResult]
  //      .mapAsync(4) { t =>
  //        for {
  //          p <- addressParse
  //          x <- CensusClient.geocode(p)
  //          y = x.right.getOrElse(CensusResult.empty)
  //          f = if (y.features.nonEmpty) y.features.head else Feature(Point(0, 0))
  //          r = TestGeocodeResult(t.inputAddress, t.x, t.y)
  //        } yield p
  //
  //    }
  //
  //
  //  }

  def toCSV: Flow[TestGeocode, String, Unit] = {
    Flow[TestGeocode]
      .map { t =>
        println(t)
        s"${t.inputAddress},${t.x},${t.y},${t.tract},${t.ax},${t.ay},${t.aFoundAddress},${t.addressMatch},${t.cx},${t.cy}"
      }
  }

  def etl(implicit ec: ExecutionContext) = {
    Flow() { implicit b =>
      import FlowGraph.Implicits._

      val address = b.add(Flow[String])
      val read = b.add(addressRead)
      //val bcast = b.add(Broadcast[TestGeocode](1))
      val points = b.add(addressPointsGeocode)
      val csv = b.add(toCSV)

      address ~> read ~> points ~> csv

      (address.inlet, csv.outlet)

    }
  }

  //Dummy function for now. Replace with real point in poly lookup
  def tractJoin(t: TestGeocode): TestGeocode = {
    val tract = "01234567890"
    TestGeocode(t.inputAddress, t.x, t.y, tract)
  }

  private def readAddress(address: String, separator: String): TestGeocode = {
    val parts = address.split(separator)
    val addr = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    TestGeocode(addr, x, y)
  }

}
