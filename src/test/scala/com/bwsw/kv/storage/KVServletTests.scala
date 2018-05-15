package com.bwsw.kv.storage

import org.scalatra.test.scalatest._

class KVServletTests extends ScalatraFunSuite {

  addServlet(classOf[KVStorageServlet], "/*")

  test("GET / on KVServlet should return status 200") {
    get("/") {
      status should equal (200)
    }
  }

}
