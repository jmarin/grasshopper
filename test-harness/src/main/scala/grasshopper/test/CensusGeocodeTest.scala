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
import grasshopper.test.etl._

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

    val source = PointGeocodeETL.addressPointsStream("address", "point")

    //var r = source
    //.map(ByteString(_))
    //.runWith(Sink.synchronousFile(new File("test-harness/target/ar-points.geojson")))

    val dir = Paths.get(System.getProperty("user.dir"))
    val path = dir.resolve("test-harness/target/ar-points.geojson")

    val r = source
      .via(PointGeocodeETL.jsonToPointInputAddress)
      .via(PointGeocodeETL.tractOverlay)
      .map(t => t.toCSV)
      .map(ByteString(_))
      .runWith(Sink.synchronousFile(new File("test-harness/target/census-results.csv")))
    //val r = source
    //.via(GeocodeETL.censusGeocodeTest)
    //.map { c =>
    //  CensusGeocodeResult(c._1, c._2).toCSV
    //}
    ////.runWith(Sink.foreach(println))
    //.map(ByteString(_))
    //.runWith(Sink.synchronousFile(new File("test-harness/target/census-results.csv")))

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
