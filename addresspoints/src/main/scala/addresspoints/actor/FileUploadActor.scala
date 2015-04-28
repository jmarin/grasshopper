package addresspoints.actor

import java.nio.file.{ Files, Path, StandardOpenOption }
import akka.actor.{ Actor, ActorLogging, Props }
import akka.http.scaladsl.model.Multipart.FormData
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.Source
import scala.util.Random

object FileUploadActor {
  case class FileUpload(formData: FormData)
  case class FileUploaded(path: Path)

  def props(): Props = {
    Props(new FileUploadActor)
  }
}

class FileUploadActor extends Actor with ActorLogging {
  import FileUploadActor._

  implicit val mat = ActorFlowMaterializer()
  implicit val ec = context.dispatcher

  override def preStart() = {
    log.info(s"FileUploadActor started at ${self.path}")
  }

  override def receive: Receive = {
    case FileUpload(formData) =>
      log.debug("File received")
      val tempDir = Files.createTempDirectory("batch_geocode")
      val tempFile = Files.createTempFile(tempDir, randomFileName(), ".tmp")
      val client = sender()
      formData.parts.runForeach { bodyPart =>
        bodyPart.entity.dataBytes.runForeach { byteString =>
          Source(List(byteString)).runForeach { e =>
            Files.write(tempFile, e.toArray, StandardOpenOption.APPEND)
          }
          client ! FileUploaded(tempFile.toAbsolutePath)
        }
      }
      log.debug(s"File uploaded: ${tempFile.toAbsolutePath}")
  }

  private def randomFileName() = {
    Random.alphanumeric.take(10).mkString
  }
}