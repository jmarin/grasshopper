package grasshopper.test

import java.nio.file.{ Files, Paths }
import java.io.File
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.io.Implicits._
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import scala.collection.JavaConverters._
import akka.util.ByteString
import grasshopper.test.model._
import feature.Feature
import spray.json._
import io.geojson.FeatureJsonProtocol._

object CensusGeocodeTest {

  implicit val system = ActorSystem("grasshopper-test-harness-census")
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val config = ConfigFactory.load()

  lazy val host = config.getString("grasshopper.test-harness.elasticsearch.host")
  lazy val port = config.getString("grasshopper.test-harness.elasticsearch.port")
  lazy val cluster = config.getString("grasshopper.test-harness.elasticsearch.cluster")

  lazy val settings = ImmutableSettings.settingsBuilder()
    .put("http.enabled", false)
    .put("node.data", false)
    .put("node.master", false)
    .put("cluster.name", cluster)
    .put("client.transport.sniff", true)

  implicit lazy val client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(host, port.toInt))

  def main(args: Array[String]): Unit = {
    println("Processing Address Points")

    val source = GeocodeETL.addressPointsStream("address", "point")

    val r = source
      .via(GeocodeETL.censusGeocodeTest)
      .runWith(Sink.foreach(println))

    r.onComplete {
      case _ =>
        println("DONE!")
        client.close()
        system.shutdown()
        Runtime.getRuntime.exit(0)
    }

    sys.addShutdownHook(system.shutdown())

  }

}
