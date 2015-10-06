package grasshopper.test

import java.io.File
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.io.Implicits._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import grasshopper.test.etl.{ CensusGeocodeETL, PointGeocodeETL }
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object TractOverlay {

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

    val source = PointGeocodeETL.addressPointsStream("arkansas", "point")

    var i = 0

    source
      .via(PointGeocodeETL.jsonToPointInputAddress)
      .via(PointGeocodeETL.tractOverlay)
      .via(CensusGeocodeETL.tractParse)
      .via(CensusGeocodeETL.geocodePoint)
      .via(CensusGeocodeETL.censusPointTractOverlay)
      .map(c => c.toCSV)
      //.runWith(Sink.foreach(println))
      .map(ByteString(_))
      .runWith(Sink.synchronousFile(new File("test-harness/target/arkansas-results.csv")))
      .onComplete {
        case _ =>
          println("DONE!")
          client.close()
          system.shutdown()
      }
  }
}