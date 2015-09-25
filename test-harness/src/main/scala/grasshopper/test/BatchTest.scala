package grasshopper.test

import java.nio.file.{ Files, Paths }
import java.io.File
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.io.Implicits._
import scala.collection.JavaConverters._
import akka.util.ByteString

object BatchTest {

  implicit val system = ActorSystem("grasshopper-test-harness")
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val dir = Paths.get(System.getProperty("user.dir"))
  val path = dir.resolve("test-harness/src/main/resources/addresses.csv")

  def main(args: Array[String]): Unit = {

    println("Processing list of addresses")

    val it = Files.lines(path).iterator()
    val source = Source(() => it.asScala)

    val r = source
      .via(GeocodeETL.geocodeAddresses)
      .via(GeocodeETL.results)
      .via(GeocodeETL.toCSV)
      .map(ByteString(_))
      .runWith(Sink.synchronousFile(new File("test-harness/target/test-harness-results.csv")))

    r.onComplete {
      case _ =>
        println("DONE!")
        system.shutdown()
        Runtime.getRuntime.exit(0)
    }

    sys.addShutdownHook(system.shutdown())
  }

}
