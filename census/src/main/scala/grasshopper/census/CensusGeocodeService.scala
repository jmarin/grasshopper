package grasshopper.census

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp
import com.typesafe.config.ConfigFactory
import grasshopper.census.tcp.TcpService
import grasshopper.metrics.JvmMetrics
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import grasshopper.census.http.HttpService

import scala.util.Properties

object CensusGeocodeService extends App with TcpService with HttpService {
  override implicit val system: ActorSystem = ActorSystem("grasshopper-census")

  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()

  override val config = ConfigFactory.load()
  override val logger = Logging(system, getClass)

  lazy val esHost = config.getString("grasshopper.census.elasticsearch.host")
  lazy val esPort = config.getString("grasshopper.census.elasticsearch.port")
  lazy val esCluster = config.getString("grasshopper.census.elasticsearch.cluster")

  lazy val settings = ImmutableSettings.settingsBuilder()
    .put("http.enabled", false)
    .put("node.data", false)
    .put("node.master", false)
    .put("cluster.name", esCluster)
    .put("client.transport.sniff", true)

  lazy val client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(esHost, esPort.toInt))

  val httpHost = config.getString("grasshopper.census.http.interface")
  val httpPort = config.getInt("grasshopper.census.http.port")

  val http = Http(system).bindAndHandle(
    routes,
    httpHost,
    httpPort
  )

  val tcpHost = config.getString("grasshopper.census.tcp.interface")
  val tcpPort = config.getInt("grasshopper.census.tcp.port")

  val tcp = Tcp().bind(tcpHost, tcpPort)
  tcp.to(tcpHandler).run()

  // Default "isMonitored" value set in "metrics" project
  lazy val isMonitored = config.getString("grasshopper.monitoring.isMonitored").toBoolean

  if (isMonitored) {
    val jvmMetrics = JvmMetrics
  }

  sys.addShutdownHook {
    client.close()
    system.shutdown()
  }

}
