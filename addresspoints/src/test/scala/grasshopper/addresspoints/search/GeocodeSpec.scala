package grasshopper.addresspoints.search

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import grasshopper.elasticsearch.ElasticsearchServer
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, FlatSpec }
import grasshopper.addresspoints.util.TestData._

class GeocodeSpec extends FlatSpec with MustMatchers with BeforeAndAfterAll with Geocode {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  val server = new ElasticsearchServer
  implicit val client = server.client

  override def beforeAll = {
    server.start()
    server.createAndWaitForIndex("address")
    server.loadFeature("address", "point", getAddressPoint1)
    client.admin().indices().refresh(new RefreshRequest("address")).actionGet()
  }

  override def afterAll = {
    server.stop()
  }

  "Address points stream search" must "return a stream of filtered addresses" in {
    fgeocode("address", "point", "45 Piney Point Ct Pottsville AR 72858", 1)
      .runWith(Sink.foreach(println))
  }

}
