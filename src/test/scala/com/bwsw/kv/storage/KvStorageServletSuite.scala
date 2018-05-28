package com.bwsw.kv.storage

import akka.actor.ActorSystem
import com.bwsw.kv.storage.error.{ConflictError, InternalError, NotFoundError}
import com.bwsw.kv.storage.processor.ElasticsearchKvProcessor
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
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val keyValueMap = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val keySomeValueMap = Map("key1" -> Some("value1"), "key2" -> Some("value2"), "key3" -> Some("value3"))
  private val keyTrueMap = Map("key1" -> true, "key2" -> true, "key3" -> true)
  private val jsonKeyValueMap = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}"
  private val index = "storage-someStorage"
  private val storage = "someStorage"
  private val `type` = "_doc"
  private val valueField = "value"
  private val keyList = List("key1", "key2", "key3")
  private val jsonKeyList = "[\"key1\",\"key2\",\"key3\"]"
  private val failureReason = "Test message"

  describe("a KvStorageServlet") {
    addServlet(new KvStorageServlet(system, processor), "/*")

    describe("(get by key)") {
      it("should get value from storage") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(Future(Right(someValue))).once
        get("/someStorage/" + someKey) {
          status should equal(200)
          body should equal(someValue)
        }
      }
      it("should return 404 Not Found if key is not present in storage") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(Future(Left(NotFoundError()))).once
        get("/someStorage/" + someKey) {
          status should equal(404)
        }
      }
      it("should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(Future(Left(InternalError(failureReason)))).once
        get("/someStorage/" + someKey) {
          status should equal(500)
        }
      }
    }

    describe("(get by keys)") {
      it("should get multiple values from storage") {
        (processor.get(_: String, _: Iterable[String])).expects(storage, keyList).returning(Future(Right(keySomeValueMap))).once
        post("/someStorage/", jsonKeyList) {
          status should equal(200)
          //        body should include ("value1")
          body should equal(jsonKeyValueMap)
        }
      }
      it("get(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.get(_: String, _: Iterable[String])).expects(storage, keyList).returning(Future(Left(InternalError(failureReason)))).once
        post("/someStorage/", jsonKeyList) {
          status should equal(500)
        }
      }
    }

    describe("(set the key/value)") {
      it("should set value by key into storage") {
        (processor.set(_: String, _: String, _: String)).expects(storage, someKey, someValue).returning(Future(Right(Unit))).once
        put("/someStorage/" + someKey, "\"" + someValue + "\"") {
          status should equal(200)
        }
      }
      it("set(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.set(_: String, _: String, _: String)).expects(storage, someKey, someValue).returning(Future(Left(InternalError(failureReason)))).once
        put("/someStorage/" + someKey, "\"" + someValue + "\"") {
          status should equal(500)
        }
      }
    }

    describe("(set key/value pairs)") {
      it("should set multiple values by keys into storage") {
        (processor.set(_: String, _: Map[String, String])).expects(storage, keyValueMap).returning(Future(Right(keyTrueMap))).once
        put("/someStorage/set", jsonKeyValueMap) {
          status should equal(200)
        }
      }
      it("set(kvs) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.set(_: String, _: Map[String, String])).expects(storage, keyValueMap).returning(Future(Left(InternalError(failureReason)))).once
        put("/someStorage/set", jsonKeyValueMap) {
          status should equal(500)
        }
      }
    }

    describe("(delete by the key)") {
      it("should delete value by key from storage") {
        (processor.delete(_: String, _: String)).expects(storage, someKey).returning(Future(Right(Unit))).once
        delete("/someStorage/" + someKey) {
          status should equal(200)
        }
      }
      it("delete(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.delete(_: String, _: String)).expects(storage, someKey).returning(Future(Left(InternalError(failureReason)))).once
        delete("/someStorage/" + someKey) {
          status should equal(500)
        }
      }
    }

    describe("(delete by keys)") {
      it("should delete multiple values by keys from storage") {
        (processor.delete(_: String, _: Iterable[String])).expects(storage, keyList).returning(Future(Right(keyTrueMap))).once
        put("/someStorage/delete", jsonKeyList) {
          status should equal(200)
        }
      }
      it("delete(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.delete(_: String, _: Iterable[String])).expects(storage, keyList).returning(Future(Left(InternalError(failureReason)))).once
        put("/someStorage/delete", jsonKeyList) {
          status should equal(500)
        }
      }
    }

    describe("(list)") {
      it("should list keys existing in storage") {
        (processor.list(_: String)).expects(storage).returning(Future(Right(keyList))).once
        get("/someStorage/list") {
          status should equal(200)
          body should equal(jsonKeyList)
        }
      }
      it("list should return 500 Internal Server Error if any request to Elasticsearch failed") {
        (processor.list(_: String)).expects(storage).returning(Future(Left(InternalError(failureReason)))).once
        get("/someStorage/list") {
          status should equal(500)
        }
      }
    }

    describe("(clear)") {
      it("should clear storage") {
        (processor.clear(_: String)).expects(storage).returning(Future(Right(Unit))).once
        put("/someStorage/clear") {
          status should equal(200)
        }
      }
      it("clear should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.clear(_: String)).expects(storage).returning(Future(Left(InternalError(failureReason)))).once
        put("/someStorage/clear") {
          status should equal(500)
        }
      }
      it("clear should return 409 Conflict Error if version of any document changed during deletion") {
        (processor.clear(_: String)).expects(storage).returning(Future(Left(ConflictError()))).once
        put("/someStorage/clear") {
          status should equal(409)
        }
      }
    }
  }

}
