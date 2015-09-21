package grasshopper.test

import java.nio.file.{ Files, Paths }

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }

import scala.collection.JavaConverters._

object BatchTest {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("grasshopper-test-harness")
    implicit val mat = ActorMaterializer()
    implicit val ec = system.dispatcher

    println("Test Harness")

    val dir = Paths.get(System.getProperty("user.dir"))
    val path = dir.resolve("test-harness/src/main/resources/addresses.csv")

    val it = Files.lines(path).iterator()
    val source = Source(() => it.asScala)
    //    source
    //      .via(GeocodeETL.addressRead)
    //      .via(GeocodeETL.overlayTract)
    //      .via(GeocodeETL.addressPointsGeocode)
    //      .via(GeocodeETL.toCSV)
    //      .runWith(Sink.foreach(println))
    //      .onComplete(_ => system.shutdown())

    source
      .via(GeocodeETL.geocodeAddresses)
      .via(GeocodeETL.results)
      .via(GeocodeETL.toCSV)
      .runWith(Sink.foreach(println))
    //.onComplete(_ => system.shutdown())

  }

}
