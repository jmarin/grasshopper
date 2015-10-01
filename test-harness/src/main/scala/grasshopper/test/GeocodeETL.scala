package grasshopper.test

import akka.stream.Supervision
import akka.stream.scaladsl._
import akka.stream.ActorAttributes.supervisionStrategy
import Supervision.resumingDecider
import com.mfglabs.stream.ExecutionContextForBlockingOps
import com.mfglabs.stream.extensions.elasticsearch.EsStream
import feature.Feature
import geometry.Point
import grasshopper.client.addresspoints.AddressPointsClient
import grasshopper.client.addresspoints.model.AddressPointsResult
import grasshopper.client.census.CensusClient
import grasshopper.client.census.model.{ CensusResult }
import grasshopper.client.parser.AddressParserClient
import grasshopper.client.parser.model.ParsedAddress
import grasshopper.test.model._
import grasshopper.test.util.Haversine
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.QueryBuilders
import scala.concurrent.ExecutionContext
import hmda.geo.client.api.HMDAGeoClient
import hmda.geo.client.api.model.census.HMDAGeoTractResult
import scala.concurrent.duration._
import scala.language.postfixOps
import spray.json._
import io.geojson.FeatureJsonProtocol._

object GeocodeETL {

  val decider: Supervision.Decider = {
    case _: NumberFormatException => Supervision.Resume
    case _ => Supervision.Stop
  }

  def addressRead(separator: String): Flow[String, PointInputAddress, Unit] = {
    Flow[String]
      .map { s =>
        val parts = s.split(separator)
        val a = parts(0)
        val x = parts(1).toDouble
        val y = parts(2).toDouble
        PointInputAddress(a, Point(x, y))
      }
  }

  def tractOverlay(implicit ec: ExecutionContext): Flow[PointInputAddress, PointInputAddressTract, Unit] = {
    Flow[PointInputAddress]
      .mapAsync(4) { i =>
        val p = i.point
        for {
          x <- HMDAGeoClient.findTractByPoint(p) if x.isRight
          y = x.right.getOrElse(HMDAGeoTractResult.empty)
          geoid = y.geoid
        } yield PointInputAddressTract(i, geoid)
      }
      .withAttributes(supervisionStrategy(decider))
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
      .withAttributes(supervisionStrategy(decider))
  }

  def addressPointsStream(index: String, indexType: String)(implicit client: Client): Source[String, Unit] = {
    implicit val ec = ExecutionContextForBlockingOps(ExecutionContext.Implicits.global)
    EsStream
      .queryAsStream(
        QueryBuilders.matchAllQuery(),
        index = index,
        `type` = indexType,
        scrollKeepAlive = 1 minutes,
        scrollSize = 1000
      )
  }

  def addressPointTractOverlay(implicit ec: ExecutionContext): Flow[AddressPointGeocode, AddressPointGeocodeTract, Unit] = {
    Flow[AddressPointGeocode]
      .mapAsync(4) { a =>
        val p = a.point
        for {
          x <- HMDAGeoClient.findTractByPoint(p) if x.isRight
          y = x.right.getOrElse(HMDAGeoTractResult.empty)
          geoid = y.geoid
        } yield AddressPointGeocodeTract(a, geoid)
      }
      .withAttributes(supervisionStrategy(decider))
  }

  def addressParse(implicit ec: ExecutionContext): Flow[PointInputAddress, CensusInputAddress, Unit] = {
    Flow[PointInputAddress]
      .mapAsync(4) { a =>
        for {
          x <- AddressParserClient.standardize(a.inputAddress)
          y = x.right.getOrElse(ParsedAddress.empty)
        } yield CensusInputAddress(y.parts.addressNumber.toInt, y.parts.streetName, y.parts.zip, y.parts.state, a.point)
      }
      .withAttributes(supervisionStrategy(decider))
  }

  def censusGeocode(implicit ec: ExecutionContext): Flow[CensusInputAddress, CensusGeocodePoint, Unit] = {
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
        } yield CensusGeocodePoint(PointInputAddress(p.toString, p.point), Point(longitude, latitude), distance)
      }
      .withAttributes(supervisionStrategy(decider))
  }

  def censusPointTractOverlay(implicit ec: ExecutionContext): Flow[CensusGeocodePoint, CensusGeocodeTract, Unit] = {
    Flow[CensusGeocodePoint]
      .mapAsync(4) { c =>
        val p = c.point
        for {
          x <- HMDAGeoClient.findTractByPoint(p) if x.isRight
          y = x.right.getOrElse(HMDAGeoTractResult.empty)
          geoid = y.geoid
        } yield CensusGeocodeTract(c, geoid)
      }
      .withAttributes(supervisionStrategy(decider))
  }

  def toCSV: Flow[TestResult, String, Unit] = {
    Flow[TestResult]
      .map { t =>
        val inputAddressTract = t.inputAddressTract
        val pointGeocode = t.addressPointTract
        val censusGeocode = t.censusGeocodeTract
        val inputAddress = t.addressPointTract.addressPointGeocode.inputAddress
        val inputLongitude = t.addressPointTract.addressPointGeocode.inputAddress.point.y
        val inputLatitude = t.addressPointTract.addressPointGeocode.inputAddress.point.x
        val pTract = t.addressPointTract.geoid
        val pLongitude = t.addressPointTract.addressPointGeocode.point.y
        val pLatitude = t.addressPointTract.addressPointGeocode.point.x
        val pFoundAddress = t.addressPointTract.addressPointGeocode.foundAddress
        val pMatch = t.addressPointTract.addressPointGeocode.addressMatch
        val pDist = t.addressPointTract.addressPointGeocode.distance
        val cLongitude = t.censusGeocodeTract.censusPointGeocode.point.y
        val cLatitude = t.censusGeocodeTract.censusPointGeocode.point.x
        val cDist = t.censusGeocodeTract.censusPointGeocode.distance
        val cTract = t.censusGeocodeTract.geoid

        s"${inputAddress.inputAddress}," +
          s"${inputAddress.point.y}," +
          s"${inputAddress.point.x}," +
          s"${inputAddressTract.geoid}," +
          s"$pFoundAddress," +
          s"$pLongitude," +
          s"$pLatitude," +
          s"$pMatch," +
          s"$pDist," +
          s"$pTract," +
          s"$cLongitude," +
          s"$cLatitude," +
          s"$cDist," +
          s"$cTract\n"
      }
  }

  def geocodeAddresses(implicit ec: ExecutionContext): Flow[String, (PointInputAddressTract, (AddressPointGeocodeTract, CensusGeocodeTract)), Unit] = {
    Flow() { implicit b =>
      import FlowGraph.Implicits._

      val address = b.add(Flow[String])
      val read = b.add(addressRead(","))
      val broadcast = b.add(Broadcast[PointInputAddress](3))
      val tract = b.add(tractOverlay)
      val point = b.add(addressPointsGeocode)
      val pointTract = b.add(addressPointTractOverlay)
      val parsedAddress = b.add(addressParse)
      val census = b.add(censusGeocode)
      val censusTract = b.add(censusPointTractOverlay)
      val zip = b.add(Zip[AddressPointGeocodeTract, CensusGeocodeTract])
      val zip1 = b.add(Zip[PointInputAddressTract, (AddressPointGeocodeTract, CensusGeocodeTract)])

      address ~> read ~> broadcast.in
      broadcast.out(0) ~> tract ~> zip1.in0
      broadcast.out(1) ~> point ~> pointTract ~> zip.in0
      broadcast.out(2) ~> parsedAddress ~> census ~> censusTract ~> zip.in1
      zip.out ~> zip1.in1
      (address.inlet, zip1.out)

    }
  }

  def totalResults: Flow[(PointInputAddressTract, (AddressPointGeocodeTract, CensusGeocodeTract)), TestResult, Unit] = {
    Flow[(PointInputAddressTract, (AddressPointGeocodeTract, CensusGeocodeTract))]
      .map(a => TestResult(a._1, a._2._1, a._2._2))
  }

  def censusTest(implicit ec: ExecutionContext): Flow[PointInputAddress ,CensusGeocodeTract, Unit] = {
    val source = addressPointsStream("address", "point")

    source.map{ s =>
      val f = s.toJson.convertTo[Feature]
      val p = f.geometry.centroid
      val a = f.get("address").getOrElse("").toString
      PointInputAddress(a, p)
    }



  }


}
