package grasshopper.addresspoints.util

import feature.Feature
import spray.json._
import io.geojson.FeatureJsonProtocol._

object TestData {
  def getAddressPoint1(): Feature = {
    val fjson = scala.io.Source.fromFile("addresspoints/src/test/resources/addresspoint1.geojson").getLines.mkString
    fjson.parseJson.convertTo[Feature]
  }
}
