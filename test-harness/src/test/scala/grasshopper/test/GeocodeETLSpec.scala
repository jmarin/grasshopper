package grasshopper.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import grasshopper.test.model.TestGeocode
import org.scalatest.{ FlatSpec, MustMatchers }

class GeocodeETLSpec extends FlatSpec with MustMatchers {
  implicit val system = ActorSystem("sys")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val addresses = List(
    "301 E Northern Lights Blvd Anchorage Alaska 99503,-149.87853,61.195315",
    "481 W Parks Hwy Wasilla Alaska 99654,-149.461945,61.582096",
    "1028 E 5th Ave Anchorage Alaska 99501,-149.864244,61.217523"
  ).toIterator

  val source = Source(() => addresses)
  //source.via(GeocodeETL.address2Feature).runWith(Sink.foreach(println))

  "Overlay with tract" should "perform spatial join" in {
    val input = TestGeocode("123 Main St", -77, 38)
    //GeocodeETL.tractJoin(input) mustBe TestGeocode("123 Main St", -77, 38, "01234567890")
  }

  "Overlay with list of tracts" should "perform spatial join" in {
    val tracts = source
      .via(GeocodeETL.addressRead)
      .via(GeocodeETL.censusOverlay)
      .map { t =>
        t.geoid10 mustBe "01234567890"
      }
  }

  //  it should "convert results to CSV" in {
  //    val address = "301 E Northern Lights Blvd Anchorage Alaska 99503,-149.87853,61.195315"
  //    val addresses = List(address).toIterator
  //    val source = Source(() => addresses)
  //    val csvList = source
  //      .via(GeocodeETL.addressRead)
  //      .via(GeocodeETL.censusOverlay)
  //      //.via(GeocodeETL.toCSV)
  //      .grouped(1)
  //      .runWith(Sink.head)
  //    csvList.foreach(c => c(0).toString mustBe s"${address},01234567890,0.0,0.0,,0.0,0.0,0.0")
  //  }

}
