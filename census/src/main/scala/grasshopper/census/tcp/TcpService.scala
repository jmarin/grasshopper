package grasshopper.census.tcp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{ Tcp, Sink, Flow }
import akka.util.ByteString
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import grasshopper.census.model.ParsedInputAddress
import grasshopper.census.protocol.CensusJsonProtocol
import grasshopper.census.search.CensusGeocode
import org.elasticsearch.client.Client
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.ExecutionContextExecutor

trait TcpService extends CensusJsonProtocol with CensusGeocode {

  lazy val tcpLog = Logger(LoggerFactory.getLogger("grasshopper-census-tcp"))

  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: ActorMaterializer
  implicit val client: Client

  def config: Config

  val tcpHandler = Sink.foreach[Tcp.IncomingConnection] { conn =>
    tcpLog.debug("Client connected from: " + conn.remoteAddress)
    conn handleWith censusFlow
  }

  val censusFlow: Flow[ByteString, ByteString, Unit] = {
    Flow[ByteString]
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
      .map(_.utf8String)
      .map(s => s.parseJson.convertTo[ParsedInputAddress])
      .map {a =>
        val results = for {
          x <- geocodeLine(client, "census", "addrfeat", a, 1) getOrElse (Nil.toArray)
        } yield x
        ByteString(results(0).toJson.toString)
      }
  }

}
