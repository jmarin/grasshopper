package grasshopper.addresspoints.search

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import feature.Feature
import grasshopper.elasticsearch.ElasticsearchServer
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.scalatest._
import util.TestData._
import spray.json._
import io.geojson.FeatureJsonProtocol._

class AddressPointGeocodeSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll with Geocode {

  val server = new ElasticsearchServer
  val client = server.client

  override def beforeAll = {
    server.start()
    server.createAndWaitForIndex("census")
    server.loadFeature("address", "point", getAddressPoint1)
    server.loadFeature("address", "point", getAddressPoint2)
    server.loadFeature("address", "point", getAddressPoint3)
    client.admin().indices().refresh(new RefreshRequest("address")).actionGet()
  }

  "Address Points streaming" must "produce features" in {
    implicit val system = ActorSystem("test-streams")
    implicit val mat = ActorMaterializer()
    implicit val ec = system.dispatcher
    val source: Source[String, Unit] = streamIndex(client, "address", "point")
    val sink = Sink.foreach(println)
    source
      .map { s =>
        val feature = s.toJson.convertTo[Feature]
        feature.get("address").getOrElse("").toString.contains("CR") mustBe true
      }
    source.runWith(sink).onComplete { _ =>
      system.shutdown()
      server.stop()
    }
  }

}
