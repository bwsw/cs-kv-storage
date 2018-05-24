package com.bwsw.kv.storage
import akka.actor.ActorSystem
import com.bwsw.kv.storage.error.{ConflictError, InternalError, NotFoundError}
import com.bwsw.kv.storage.processor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.http.{ElasticError, RequestFailure}
import org.scalatra.test.scalatest._
import org.scalatest.FunSpecLike
import org.scalamock.scalatest.MockFactory

import scala.concurrent.Future

class KvStorageServletSuite
  extends ScalatraSuite
  with FunSpecLike
  with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global
  private val system = ActorSystem()
  private val processor = mock[ElasticsearchKvProcessor]
  private def getRequestFailure: RequestFailure = {
    RequestFailure(500, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))
  }

  describe("a KvStorageServlet"){
    addServlet(new KvStorageServlet(system, processor), "/*")

    // get /:storage_uuid/:key start
    it("should get value from storage") {
      (processor.get (_: String,_: String)).expects("someStorage", "someKey").returning(Future(Right("someValue"))).once
      get("/someStorage/someKey") {
        status should equal (200)
        body should equal ("someValue")
      }
    }
    it("should return 404 Not Found if key is not present in storage") {
      (processor.get (_: String,_: String)).expects("someStorage", "someKey").returning(Future(Left(NotFoundError()))).once
      get("/someStorage/someKey") {
        status should equal (404)
      }
    }
    it("should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.get (_: String,_: String)).expects("someStorage", "someKey").returning(Future(Left(InternalError(getRequestFailure)))).once
      get("/someStorage/someKey") {
        status should equal (500)
      }
    }
    // get /:storage_uuid/:key end

    // post /:storage_uuid/ start
    it("should get multiple values from storage") {
      (processor.get (_: String,_: Iterable[String])).expects("someStorage", List("key1", "key2", "key3")).returning(Future(Right(Map(
        "key1" -> Some("value1"), "key2" -> Some("value2"), "key3" -> Some("value3"))))).once
      post("/someStorage/", "[\"key1\",\"key2\",\"key3\"]") {
        status should equal (200)
//        body should include ("value1")
        body should equal ("{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}")
      }
    }
    it("get(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.get (_: String,_: Iterable[String])).expects("someStorage", List("key1", "key2", "key3")).returning(Future(Left(InternalError(getRequestFailure)))).once
      post("/someStorage/", "[\"key1\",\"key2\",\"key3\"]") {
        status should equal (500)
      }
    }
    // post /:storage_uuid/ end

    // put /:storage_uuid/:key start
    it("should set value by key into storage") {
      (processor.set (_: String,_: String, _: String)).expects("someStorage", "someKey", "someValue").returning(Future(Right(Unit))).once
      put("/someStorage/someKey", "\"someValue\"") {
        status should equal (200)
      }
    }
    it("set(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.set (_: String,_: String, _: String)).expects("someStorage", "someKey", "someValue").returning(Future(Left(InternalError(getRequestFailure)))).once
      put("/someStorage/someKey", "\"someValue\"") {
        status should equal (500)
      }
    }
    // put /:storage_uuid/:key end

    // put /:storage_uuid/ start
    it("should set multiple values by keys into storage") {
      (processor.set (_: String,_: Iterable[(String, String)])).expects("someStorage",  Map(
        "key1" -> "value1", "key2" -> "value2", "key3" -> "value3")).returning(Future(Right(Map(
        "key1" -> true, "key2" -> true, "key3" -> true)))).once
      put("/someStorage/set", "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}") {
        status should equal (200)
      }
    }
    it("set(kvs) should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.set (_: String,_: Iterable[(String, String)])).expects("someStorage", Map(
        "key1" -> "value1", "key2" -> "value2", "key3" -> "value3")).returning(Future(Left(InternalError(getRequestFailure)))).once
      put("/someStorage/set", "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}") {
        status should equal (500)
      }
    }
    // put /:storage_uuid/ end

    // delete /:storage_uuid/:key start
    it("should delete value by key from storage") {
      (processor.delete (_: String,_: String)).expects("someStorage", "someKey").returning(Future(Right(Unit))).once
      delete("/someStorage/someKey") {
        status should equal (200)
      }
    }
    it("delete(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.delete (_: String,_: String)).expects("someStorage", "someKey").returning(Future(Left(InternalError(getRequestFailure)))).once
      delete("/someStorage/someKey") {
        status should equal (500)
      }
    }
    // delete /:storage_uuid/:key end

    // put /:storage_uuid/delete start
    it("should delete multiple values by keys from storage") {
      (processor.delete (_: String,_: Iterable[String])).expects("someStorage", List("key1", "key2", "key3")).returning(Future(Right(Map(
        "key1" -> true, "key2" -> true, "key3" -> true)))).once
      put("/someStorage/delete", "[\"key1\",\"key2\",\"key3\"]") {
        status should equal (200)
      }
    }
    it("delete(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.delete (_: String,_: Iterable[String])).expects("someStorage", List("key1", "key2", "key3")).returning(Future(Left(InternalError(getRequestFailure)))).once
      put("/someStorage/delete", "[\"key1\",\"key2\",\"key3\"]") {
        status should equal (500)
      }
    }
    // put /:storage_uuid/delete end

    // get /:storage_uuid/list start
    it("should list keys existing in storage") {
      (processor.list (_: String)).expects("someStorage").returning(Future(Right(List("key1", "key2", "key3")))).once
      get("/someStorage/list") {
        status should equal (200)
        body should equal ("[\"key1\",\"key2\",\"key3\"]")
      }
    }
    it("list should return 500 Internal Server Error if any request to Elasticsearch failed") {
      (processor.list (_: String)).expects("someStorage").returning(Future(Left(InternalError(getRequestFailure)))).once
      get("/someStorage/list") {
        status should equal(500)
      }
    }
    // get /:storage_uuid/list end

    // post /:storage_uuid/clear start
    it("should clear storage") {
      (processor.clear (_: String)).expects("someStorage").returning(Future(Right(Unit))).once
      put("/someStorage/clear") {
        status should equal (200)
      }
    }
    it("clear should return 500 Internal Server Error if request to Elasticsearch failed") {
      (processor.clear (_: String)).expects("someStorage").returning(Future(Left(InternalError(getRequestFailure)))).once
      put("/someStorage/clear") {
        status should equal(500)
      }
    }
    it("clear should return 409 Conflict Error if version of any document changed during deletion") {
      (processor.clear (_: String)).expects("someStorage").returning(Future(Left(ConflictError()))).once
      put("/someStorage/clear") {
        status should equal(409)
      }
    }
    // post /:storage_uuid/clear end
  }

}
