//package grasshopper.test
//
//import java.io.File
//import java.nio.file.{ Files, Paths }
//import akka.actor.ActorSystem
//import akka.stream.scaladsl.{ Sink, Source }
//import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Supervision }
//import akka.stream.io.Implicits._
//import akka.util.ByteString
//import scala.collection.JavaConverters._
//
//object CAI_Test {
//
//  implicit val system = ActorSystem("grasshopper-test-harness")
//  val decider: Supervision.Decider = {
//    //case _: NumberFormatException => Supervision.Resume
//    case _ => Supervision.Resume
//  }
//  implicit val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
//  implicit val ec = system.dispatcher
//
//  def main(args: Array[String]): Unit = {
//    println("Processing Community Anchor Institutions")
//
//    val dir = Paths.get(System.getProperty("user.dir"))
//    val path = dir.resolve("/Users/marinj/Downloads/addr_test.csv")
//
//    val it = Files.lines(path).iterator()
//    val source = Source(() => it.asScala)
//
//    source
//      .via(GeocodeETL.geocodeAddresses)
//      .via(GeocodeETL.totalResults)
//      .via(GeocodeETL.toCSV)
//      .map(ByteString(_))
//      .runWith(Sink.synchronousFile(new File("test-harness/target/addr6k-results.csv")))
//      //.runWith(Sink.foreach(println))
//      .onComplete {
//        case _ =>
//          println("DONE!")
//          system.shutdown()
//      }
//
//  }
//}
