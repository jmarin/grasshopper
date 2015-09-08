package grasshopper.test

import akka.stream.scaladsl._
import geometry.Point
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.client.census.CensusClient
import grasshopper.client.census.model.{ CensusResult, ParsedInputAddress }
import grasshopper.client.parser.AddressParserClient
import grasshopper.client.parser.model.ParsedAddress
import grasshopper.test.model._

import scala.concurrent.ExecutionContext

object GeocodeETL {

  def addressRead: Flow[String, InputAddress, Unit] = {
    Flow[String]
      .map(a => readAddress(a, ","))
  }

  def censusOverlay: Flow[InputAddress, CensusOverlay, Unit] = {
    Flow[InputAddress]
      .map(t => tractJoin(t))
  }

  def addressPointsGeocode(implicit ec: ExecutionContext): Flow[InputAddress, AddressPointGeocode, Unit] = {
    Flow[InputAddress]
      .mapAsync(4) { t =>
        val a = t.inputAddress
        for {
          x <- AddressPointsClient.geocode(a) if x.isRight
          y = x.right.getOrElse(AddressPointsResult.empty)
          features = y.features.toList
          longitude = if (features.nonEmpty) features.head.geometry.centroid.x else 0
          latitude = if (features.nonEmpty) features.head.geometry.centroid.y else 0
          foundAddress = if (features.nonEmpty) features.head.get("address").getOrElse("").toString else ""
          matchAddress = if (features.nonEmpty) features.head.get("match").getOrElse(0).toString.toDouble else 0
        } yield AddressPointGeocode(t, Point(longitude, latitude), foundAddress, matchAddress)
      }
  }

  def addressParse(implicit ec: ExecutionContext): Flow[InputAddress, ParsedInputAddress, Unit] = {
    Flow[InputAddress]
      .mapAsync(4) { a =>
        for {
          x <- AddressParserClient.standardize(a.inputAddress)
          y = x.right.getOrElse(ParsedAddress.empty)
          p = ParsedInputAddress(y.parts.addressNumber.toInt, y.parts.streetName, y.parts.zip, y.parts.state)
        } yield p
      }
  }

  def censusGeocode(implicit ec: ExecutionContext): Flow[ParsedInputAddress, CensusGeocode, Unit] = {
    Flow[ParsedInputAddress]
      .mapAsync(4) { p =>
        val a = p.toString
        for {
          x <- CensusClient.geocode(p)
          y = x.right.getOrElse(CensusResult.empty)
          features = y.features.toList
          longitude = if (features.nonEmpty) features.head.geometry.centroid.x else 0
          latitude = if (features.nonEmpty) features.head.geometry.centroid.y else 0
        } yield CensusGeocode(InputAddress(p.toString, Point(0, 0)), Point(longitude, latitude))
      }
  }

  def toCSV: Flow[(ParsedInputAddress, TestGeocode), String, Unit] = {
    Flow[(ParsedInputAddress, TestGeocode)]
      .map { t =>
        s"${t._2.inputAddress}" +
          s",${t._2.x}" +
          s",${t._2.y}" +
          s",${t._2.tract}" +
          s",${t._2.ax}" +
          s",${t._2.ay}" +
          s",${t._2.aFoundAddress}" +
          s",${t._2.addressMatch}" +
          s",${t._2.cx}" +
          s",${t._2.cy}"
      }
  }

  def geocodeAddresses(implicit ec: ExecutionContext): Flow[String, (AddressPointGeocode, CensusGeocode), Unit] = {
    Flow() { implicit b =>
      import FlowGraph.Implicits._

      val address = b.add(Flow[String])
      val read = b.add(addressRead)
      val broadcast = b.add(Broadcast[InputAddress](2))
      val points = b.add(addressPointsGeocode)
      val parsedAddress = b.add(addressParse)
      val census = b.add(censusGeocode)
      val zip = b.add(Zip[AddressPointGeocode, CensusGeocode])

      address ~> read ~> broadcast.in
      broadcast.out(0) ~> points ~> zip.in0
      broadcast.out(1) ~> parsedAddress ~> census ~> zip.in1

      (address.inlet, zip.out)

    }
  }

  //Dummy function for now. Replace with real point in poly lookup
  def tractJoin(a: InputAddress): CensusOverlay = {
    val tract = "01234567890"
    CensusOverlay(a, tract)
  }

  private def readAddress(address: String, separator: String): InputAddress = {
    val parts = address.split(separator)
    val a = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    InputAddress(a, Point(x, y))
  }

}
