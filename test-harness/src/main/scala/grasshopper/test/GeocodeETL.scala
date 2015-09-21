package grasshopper.test

import akka.stream.scaladsl._
import geometry.Point
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.client.census.CensusClient
import grasshopper.client.census.model.{ CensusResult }
import grasshopper.client.parser.AddressParserClient
import grasshopper.client.parser.model.ParsedAddress
import grasshopper.test.model._
import grasshopper.test.util.Haversine

import scala.concurrent.ExecutionContext

object GeocodeETL {

  def addressRead: Flow[String, PointInputAddress, Unit] = {
    Flow[String]
      .map(a => readAddress(a, ","))
  }

  def censusOverlay: Flow[AddressPointGeocode, AddressPointGeocodeTract, Unit] = {
    Flow[AddressPointGeocode]
      .map(t => tractJoin(t))
  }

  def addressPointsGeocode(implicit ec: ExecutionContext): Flow[PointInputAddress, AddressPointGeocode, Unit] = {
    Flow[PointInputAddress]
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
          distance = Haversine.distance(Point(longitude, latitude), t.point)
        } yield AddressPointGeocode(t, Point(longitude, latitude), foundAddress, matchAddress, distance)
      }
  }

  def addressParse(implicit ec: ExecutionContext): Flow[PointInputAddress, CensusInputAddress, Unit] = {
    Flow[PointInputAddress]
      .mapAsync(4) { a =>
        for {
          x <- AddressParserClient.standardize(a.inputAddress)
          y = x.right.getOrElse(ParsedAddress.empty)
          p = CensusInputAddress(y.parts.addressNumber.toInt, y.parts.streetName, y.parts.zip, y.parts.state, a.point)
        } yield p
      }
  }

  def censusGeocode(implicit ec: ExecutionContext): Flow[CensusInputAddress, CensusGeocode, Unit] = {
    Flow[CensusInputAddress]
      .mapAsync(4) { p =>
        val a = p.toString
        for {
          x <- CensusClient.geocode(grasshopper.client.census.model.ParsedInputAddress(p.number, p.streetName, p.zipCode, p.state))
          y = x.right.getOrElse(CensusResult.empty)
          features = y.features.toList
          longitude = if (features.nonEmpty) features.head.geometry.centroid.x else 0
          latitude = if (features.nonEmpty) features.head.geometry.centroid.y else 0
          distance = Haversine.distance(Point(longitude, latitude), p.point)
        } yield CensusGeocode(PointInputAddress(p.toString, p.point), Point(longitude, latitude), distance)
      }
  }

  def toCSV: Flow[TestResult, String, Unit] = {
    Flow[TestResult]
      .map { t =>
        val pointGeocode = t.addressPointTract
        val censusGeocode = t.censusGeocode
        val inputAddress = t.addressPointTract.addressPointGeocode.inputAddress
        val inputLongitude = t.addressPointTract.addressPointGeocode.inputAddress.point.y
        val inputLatitude = t.addressPointTract.addressPointGeocode.inputAddress.point.x
        val tract = t.addressPointTract.geoid10
        val pLongitude = t.addressPointTract.addressPointGeocode.point.y
        val pLatitude = t.addressPointTract.addressPointGeocode.point.x
        val pFoundAddress = t.addressPointTract.addressPointGeocode.foundAddress
        val pMatch = t.addressPointTract.addressPointGeocode.addressMatch
        val pDist = t.addressPointTract.addressPointGeocode.distance
        val cLongitude = t.censusGeocode.point.y
        val cLatitude = t.censusGeocode.point.x
        val cDist = t.censusGeocode.distance

        s"${inputAddress.inputAddress}," +
          s"${inputAddress.point.y}," +
          s"${inputAddress.point.x}," +
          s"$pLongitude," +
          s"$pLatitude," +
          s"$pFoundAddress," +
          s"$pMatch," +
          s"$pDist," +
          s"$tract," +
          s"$cLongitude," +
          s"$cLatitude," +
          s"$cDist"
      }
  }

  def geocodeAddresses(implicit ec: ExecutionContext): Flow[String, (AddressPointGeocodeTract, CensusGeocode), Unit] = {
    Flow() { implicit b =>
      import FlowGraph.Implicits._

      val address = b.add(Flow[String])
      val read = b.add(addressRead)
      val broadcast = b.add(Broadcast[PointInputAddress](2))
      val points = b.add(addressPointsGeocode)
      val tracts = b.add(censusOverlay)
      val parsedAddress = b.add(addressParse)
      val census = b.add(censusGeocode)
      val zip = b.add(Zip[AddressPointGeocodeTract, CensusGeocode])

      address ~> read ~> broadcast.in
      broadcast.out(0) ~> points ~> tracts ~> zip.in0
      broadcast.out(1) ~> parsedAddress ~> census ~> zip.in1

      (address.inlet, zip.out)

    }
  }

  def results: Flow[(AddressPointGeocodeTract, CensusGeocode), TestResult, Unit] = {
    Flow[(AddressPointGeocodeTract, CensusGeocode)].map(a => TestResult(a._1, a._2))
  }

  //Dummy function for now. Replace with real point in poly lookup
  def tractJoin(a: AddressPointGeocode): AddressPointGeocodeTract = {
    val tract = "01234567890"
    AddressPointGeocodeTract(a, tract)
  }

  private def readAddress(address: String, separator: String): PointInputAddress = {
    val parts = address.split(separator)
    val a = parts(0)
    val x = parts(1).toDouble
    val y = parts(2).toDouble
    PointInputAddress(a, Point(x, y))
  }

}
