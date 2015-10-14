package grasshopper.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
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

}
