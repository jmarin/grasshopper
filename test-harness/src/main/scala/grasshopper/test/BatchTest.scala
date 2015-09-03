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
    source
      .via(GeocodeETL.address2GeocodeTestResult)
      .via(GeocodeETL.overlayTract)
      //.via(GeocodeETL.addressPointsFlow)
      //.via(GeocodeETL.toCsv)
      .runWith(Sink.foreach(println))

    //    val source = Source
    //      .synchronousFile(testFile)
    //      //.via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true))
    //      .map(_.utf8String)
    //      .runWith(Sink.onComplete(_ => system.shutdown()))

  }

}
