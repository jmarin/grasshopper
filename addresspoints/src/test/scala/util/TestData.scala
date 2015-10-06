package util

import feature._
import io.geojson.FeatureJsonProtocol._
import spray.json._

object TestData {
  def getAddressPoint1(): Feature = {
    val fjson = scala.io.Source.fromFile("addresspoints/src/test/resources/address_point1.geojson").getLines.mkString
    fjson.parseJson.convertTo[Feature]
  }

  def getAddressPoint2(): Feature = {
    val fjson = scala.io.Source.fromFile("addresspoints/src/test/resources/address_point2.geojson").getLines.mkString
    fjson.parseJson.convertTo[Feature]
  }

  def getAddressPoint3(): Feature = {
    val fjson = scala.io.Source.fromFile("addresspoints/src/test/resources/address_point3.geojson").getLines.mkString
    fjson.parseJson.convertTo[Feature]
  }
}
