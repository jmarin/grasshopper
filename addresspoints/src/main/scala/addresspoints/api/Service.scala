package addresspoints.api

import java.net.InetAddress
import java.nio.file.Files
import java.time.Instant
import addresspoints.actor.FileUploadActor
import addresspoints.model.{ AddressInput, Status }
import addresspoints.protocol.JsonProtocol
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.coding.{ Deflate, Gzip, NoCoding }
import akka.http.marshalling.ToResponseMarshallable
import akka.http.marshallers.sprayjson.SprayJsonSupport._
import akka.http.model.Multipart.FormData
import akka.http.model.StatusCodes._
import akka.http.server.Directives._
import akka.http.server.StandardRoute
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{ Sink, Flow, Source }
import akka.util.Timeout
import com.typesafe.config.Config
import grasshopper.elasticsearch.Geocode
import org.elasticsearch.client.Client
import scala.concurrent.ExecutionContextExecutor
import scala.util.{ Failure, Success }
import spray.json._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import io.geojson.FeatureJsonProtocol._
import scala.concurrent.duration._
import akka.pattern.ask
import scala.collection.JavaConverters._
import feature._

trait Service extends JsonProtocol with Geocode {
  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: ActorFlowMaterializer
  implicit val client: Client

  def config: Config

  val logger: LoggingAdapter

  override lazy val log = Logger(LoggerFactory.getLogger("grasshopper-address-points"))

  val routes = {
    path("upload") {
      get {
        getFromResource("web/index.html")
      }
    } ~
      path("status") {
        get {
          encodeResponseWith(NoCoding, Gzip, Deflate) {
            complete {
              // Creates ISO-8601 date string in UTC down to millisecond precision
              val now = Instant.now.toString
              val host = InetAddress.getLocalHost.getHostName
              val status = Status("OK", now, host)
              log.info(status.toJson.toString())
              ToResponseMarshallable(status)
            }
          }
        }
      } ~
      pathPrefix("addresses") {
        pathPrefix("points") {
          path("batch") {
            post {
              implicit val timeout = Timeout(5.seconds)
              import FileUploadActor._
              val fileUploadActor = system.actorSelection("/user/file-upload")

              entity(as[FormData]) { formData =>
                onSuccess(fileUploadActor ? FileUpload(formData)) {
                  case fileUploaded: FileUploaded =>
                    val it = Files.lines(fileUploaded.path).iterator()
                    val source = Source(() => it.asScala)

                    val flow = Flow[String]
                      .map(a => geocode(client, "address", "point", a, 1))
                      .map[Feature] {
                        case Success(f) =>
                          if (f.size > 0) f(0) else emptyFeature("error")
                        case Failure(_) =>
                          log.error("Failure to geocode address")
                          emptyFeature("error")
                      }

                    encodeResponseWith(NoCoding, Gzip, Deflate) {
                      complete {
                        source
                          .via(flow)
                          .grouped(100000)
                          .runWith(Sink.head)
                          .map(e => ToResponseMarshallable(e))
                      }
                    }
                  case _ =>
                    complete(BadRequest)

                }
              }
            }
          } ~
            post {
              encodeResponseWith(NoCoding, Gzip, Deflate) {
                entity(as[String]) { json =>
                  try {
                    val addressInput = json.parseJson.convertTo[AddressInput]
                    geocodePoints(addressInput.address, 1)
                  } catch {
                    case e: spray.json.DeserializationException =>
                      complete(BadRequest)
                  }
                }
              }
            } ~
            get {
              path(Segment) { address =>
                parameters('suggest.as[Int] ? 1) { suggest =>
                  get {
                    encodeResponseWith(NoCoding, Gzip, Deflate) {
                      geocodePoints(address, suggest)
                    }
                  }
                }
              }
            }
        }
      }
  }

  private def geocodePoints(address: String, count: Int): StandardRoute = {
    val points = geocode(client, "address", "point", address, count)
    points match {
      case Success(pts) =>
        if (pts.size > 0) {
          complete {
            ToResponseMarshallable(pts)
          }
        } else {
          complete(NotFound)
        }
      case Failure(_) =>
        complete {
          NotFound
        }
    }
  }
}
