package com.bwsw.kv.storage

import org.scalatra.test.scalatest._

class KvStorageServletTests extends ScalatraFunSuite {

  addServlet(classOf[KvStorageServlet], "/*")

//  test("GET / on KVServlet should return status 200") {
//    get("/") {
//      status should equal (200)
//    }
//  }

}
