package grasshopper.test.etl

import akka.stream.Supervision
import akka.stream.scaladsl._
import akka.stream.OverflowStrategy
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
import grasshopper.test.model.GeocodeModel._
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

object PointGeocodeETL {

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
      .mapAsyncUnordered(4) { i =>
        val p = i.point
        for {
          x <- HMDAGeoClient.findTractByPoint(p) if x.isRight
          y = x.right.getOrElse(HMDAGeoTractResult.empty)
          geoid = y.geoid
        } yield PointInputAddressTract(i, geoid)
      }
      .withAttributes(supervisionStrategy(resumingDecider))
  }

  def addressPointsGeocode(implicit ec: ExecutionContext): Flow[PointInputAddress, AddressPointGeocode, Unit] = {
    Flow[PointInputAddress]
      .mapAsyncUnordered(4) { t =>
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
      .withAttributes(supervisionStrategy(resumingDecider))
  }

  def addressPointsStream(index: String, indexType: String)(implicit client: Client): Source[String, Unit] = {
    implicit val ec = ExecutionContextForBlockingOps(ExecutionContext.Implicits.global)
    EsStream
      .queryAsStream(
        QueryBuilders.matchAllQuery(),
        index = index,
        `type` = indexType,
        scrollKeepAlive = 1 minutes,
        scrollSize = 10
      )
  }

  def addressPointTractOverlay(implicit ec: ExecutionContext): Flow[AddressPointGeocode, AddressPointGeocodeTract, Unit] = {
    Flow[AddressPointGeocode]
      .mapAsyncUnordered(4) { a =>
        val p = a.point
        for {
          x <- HMDAGeoClient.findTractByPoint(p) if x.isRight
          y = x.right.getOrElse(HMDAGeoTractResult.empty)
          geoid = y.geoid
        } yield AddressPointGeocodeTract(a, geoid)
      }
      .withAttributes(supervisionStrategy(resumingDecider))
  }

  def jsonToPointInputAddress: Flow[String, PointInputAddress, Unit] = {
    Flow[String]
      .map { s =>
        val f = s.parseJson.convertTo[Feature]
        val p = f.geometry.centroid
        val a = f.get("address").getOrElse("").toString
        PointInputAddress(a, p)
      }
  }

}
