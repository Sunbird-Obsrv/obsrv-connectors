package org.sunbird.obsrv.kafkaconnector.fixtures

object EventFixture {
  val VALID_JSON_EVENT = """{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}"""
  val VALID_JSON_EVENT_ARRAY = """[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]"""
  val INVALID_JSON_EVENT = """{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}"""
}
