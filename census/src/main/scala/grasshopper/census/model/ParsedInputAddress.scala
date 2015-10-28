package grasshopper.census.model

case class ParsedInputAddress(addressNumber: String, streetName: String, zipCode: String, city: String, state: String) {
  override def toString(): String = {
    s"${addressNumber} ${streetName} ${city} ${state} ${zipCode}"
  }
}

object ParsedInputAddress {
  def empty: ParsedInputAddress =
    ParsedInputAddress("", "", "", "", "")
}
