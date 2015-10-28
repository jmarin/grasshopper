package grasshopper.census.protocol

import grasshopper.census.model._
import spray.json.DefaultJsonProtocol
import io.geojson.FeatureJsonProtocol._

trait CensusJsonProtocol extends DefaultJsonProtocol {
  implicit val statusFormat = jsonFormat4(Status.apply)
  implicit val addressInputFormat = jsonFormat5(ParsedInputAddress.apply)
  implicit val censusResultFormat = jsonFormat2(CensusResult.apply)
}