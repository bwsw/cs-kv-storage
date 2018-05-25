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
  private val Key = "someKey"
  private val Value = "someValue"
  private val KeyValueMap = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val KeySomeValueMap = Map("key1" -> Some("value1"), "key2" -> Some("value2"), "key3" -> Some("value3"))
  private val KeyTrueMap = Map("key1" -> true, "key2" -> true, "key3" -> true)
  private val JsonKeyValueMap = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}"
  private val Index = "storage-someStorage"
  private val Storage = "someStorage"
  private val `type` = "_doc"
  private val ValueField = "value"
  private val KeyList = List("key1", "key2", "key3")
  private val JsonKeyList = "[\"key1\",\"key2\",\"key3\"]"
  private val FailureReason = "Test message"

  describe("a KvStorageServlet") {
    addServlet(new KvStorageServlet(system, processor), "/*")

    describe("(get by key)") {
      it("should get value from storage") {
        (processor.get(_: String, _: String)).expects(Storage, Key).returning(Future(Right(Value))).once
        get("/someStorage/" + Key) {
          status should equal(200)
          body should equal(Value)
        }
      }
      it("should return 404 Not Found if key is not present in storage") {
        (processor.get(_: String, _: String)).expects(Storage, Key).returning(Future(Left(NotFoundError()))).once
        get("/someStorage/" + Key) {
          status should equal(404)
        }
      }
      it("should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.get(_: String, _: String)).expects(Storage, Key).returning(Future(Left(InternalError(FailureReason)))).once
        get("/someStorage/" + Key) {
          status should equal(500)
        }
      }
    }

    describe("(get by keys)") {
      it("should get multiple values from storage") {
        (processor.get(_: String, _: Iterable[String])).expects(Storage, KeyList).returning(Future(Right(KeySomeValueMap))).once
        post("/someStorage/", JsonKeyList) {
          status should equal(200)
          //        body should include ("value1")
          body should equal(JsonKeyValueMap)
        }
      }
      it("get(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.get(_: String, _: Iterable[String])).expects(Storage, KeyList).returning(Future(Left(InternalError(FailureReason)))).once
        post("/someStorage/", JsonKeyList) {
          status should equal(500)
        }
      }
    }

    describe("(set the key/value)") {
      it("should set value by key into storage") {
        (processor.set(_: String, _: String, _: String)).expects(Storage, Key, Value).returning(Future(Right(Unit))).once
        put("/someStorage/" + Key, "\"" + Value + "\"") {
          status should equal(200)
        }
      }
      it("set(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.set(_: String, _: String, _: String)).expects(Storage, Key, Value).returning(Future(Left(InternalError(FailureReason)))).once
        put("/someStorage/" + Key, "\"" + Value + "\"") {
          status should equal(500)
        }
      }
    }

    describe("(set key/value pairs)") {
      it("should set multiple values by keys into storage") {
        (processor.set(_: String, _: Map[String, String])).expects(Storage, KeyValueMap).returning(Future(Right(KeyTrueMap))).once
        put("/someStorage/set", JsonKeyValueMap) {
          status should equal(200)
        }
      }
      it("set(kvs) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.set(_: String, _: Map[String, String])).expects(Storage, KeyValueMap).returning(Future(Left(InternalError(FailureReason)))).once
        put("/someStorage/set", JsonKeyValueMap) {
          status should equal(500)
        }
      }
    }

    describe("(delete by the key)") {
      it("should delete value by key from storage") {
        (processor.delete(_: String, _: String)).expects(Storage, Key).returning(Future(Right(Unit))).once
        delete("/someStorage/" + Key) {
          status should equal(200)
        }
      }
      it("delete(key) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.delete(_: String, _: String)).expects(Storage, Key).returning(Future(Left(InternalError(FailureReason)))).once
        delete("/someStorage/" + Key) {
          status should equal(500)
        }
      }
    }

    describe("(delete by keys)") {
      it("should delete multiple values by keys from storage") {
        (processor.delete(_: String, _: Iterable[String])).expects(Storage, KeyList).returning(Future(Right(KeyTrueMap))).once
        put("/someStorage/delete", JsonKeyList) {
          status should equal(200)
        }
      }
      it("delete(keys) should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.delete(_: String, _: Iterable[String])).expects(Storage, KeyList).returning(Future(Left(InternalError(FailureReason)))).once
        put("/someStorage/delete", JsonKeyList) {
          status should equal(500)
        }
      }
    }

    describe("(list)") {
      it("should list keys existing in storage") {
        (processor.list(_: String)).expects(Storage).returning(Future(Right(KeyList))).once
        get("/someStorage/list") {
          status should equal(200)
          body should equal(JsonKeyList)
        }
      }
      it("list should return 500 Internal Server Error if any request to Elasticsearch failed") {
        (processor.list(_: String)).expects(Storage).returning(Future(Left(InternalError(FailureReason)))).once
        get("/someStorage/list") {
          status should equal(500)
        }
      }
    }

    describe("(clear)") {
      it("should clear storage") {
        (processor.clear(_: String)).expects(Storage).returning(Future(Right(Unit))).once
        put("/someStorage/clear") {
          status should equal(200)
        }
      }
      it("clear should return 500 Internal Server Error if request to Elasticsearch failed") {
        (processor.clear(_: String)).expects(Storage).returning(Future(Left(InternalError(FailureReason)))).once
        put("/someStorage/clear") {
          status should equal(500)
        }
      }
      it("clear should return 409 Conflict Error if version of any document changed during deletion") {
        (processor.clear(_: String)).expects(Storage).returning(Future(Left(ConflictError()))).once
        put("/someStorage/clear") {
          status should equal(409)
        }
      }
    }
  }

}
