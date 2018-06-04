package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.error._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest._

import scala.concurrent.Future

class KvStorageServletSuite
  extends ScalatraSuite
    with FunSpecLike
    with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val system = ActorSystem()
  private val processor = mock[KvProcessor]
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val keyValues = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val keys = List("key1", "key2", "key3")
  private val jsonKeys = "[\"key1\",\"key2\",\"key3\"]"
  private val jsonKeyValues = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}"
  private val jsonNullValues = "{\"key1\":null,\"key2\":null,\"key3\":null}"
  private val jsonKeyResult = "{\"key1\":true,\"key2\":true,\"key3\":true}"
  private val storage = "someStorage"
  private val internalError = Future(Left(InternalError("some reason")))
  private val jsonHeaders = Map("Content-Type" -> "application/json")
  private val textHeaders = Map("Content-Type" -> "text/plain")

  describe("a KvStorageServlet") {
    addServlet(new KvStorageServlet(system, processor), "/*")

    describe("(get by key)") {
      val path = s"/get/$storage/$someKey"

      it("should return the value if the key exists") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(Future(Right(someValue))).once
        get(path, Seq(), Map()) {
          status should equal(200)
          body should equal(someValue)
          response.getContentType should include("text/plain")
        }
      }

      it("should return 404 Not Found if the key does not exist") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(Future(Left(NotFoundError()))).once
        get(path, Seq(), Map()) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.get(_: String, _: String)).expects(storage, someKey).returning(internalError).once
        get(path, Seq(), Map()) {
          status should equal(500)
        }
      }
    }

    describe("(get by keys)") {
      val path = "/get/" + storage

      def testSuccess(result: Map[String, Option[String]], expectedBody: String) = {
        (processor.get(_: String, _: Iterable[String])).expects(storage, keys).returning(Future(Right(result))).once
        post(path, jsonKeys, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
        (processor.get(_: String, _: Iterable[String])).expects(storage, keys).never
        post(path, body, headers) {
          status should equal(400)
        }
      }

      it("should get values by keys") {
        testSuccess(keyValues.map(kv => (kv._1, Some(kv._2))), jsonKeyValues)
      }

      it("should return null for keys that do not exist") {
        testSuccess(keyValues.map(kv => (kv._1, None)), jsonNullValues)
      }

      it("should return 400 Bad Request Error if Content-Type is not application/json") {
        testBadRequest(jsonKeys, textHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid JSON") {
        testBadRequest("[\"key\"", jsonHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid") {
        testBadRequest("{\"field\":\"value\"}", jsonHeaders)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.get(_: String, _: Iterable[String])).expects(storage, keys).returning(internalError).once
        post(path, jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set the key/value)") {
      val path = s"/set/$storage/$someKey"

      it("should set the value by the key") {
        (processor.set(_: String, _: String, _: String)).expects(storage, someKey, someValue)
          .returning(Future(Right(Unit))).once
        put(path, someValue, textHeaders) {
          status should equal(200)
          response.getContentType should include("text/plain")
        }
      }

      it("should return 400 Bad Request Error if Content-Type is not text/plain") {
        put(path, someValue, jsonHeaders) {
          status should equal(400)
        }
      }

      it("should return 400 Bad Request Error if the key or value are invalid") {
        (processor.set(_: String, _: String, _: String)).expects(storage, someKey, someValue)
          .returning(Future(Left(BadRequestError())))
        put(path, someValue, textHeaders) {
          status should equal(400)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.set(_: String, _: String, _: String)).expects(storage, someKey, someValue)
          .returning(internalError).once
        put(path, someValue, textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set key/value pairs)") {
      val path = s"/set/$storage"

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        (processor.set(_: String, _: Map[String, String])).expects(storage, keyValues).returning(Future(Right(result)))
          .once
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
        (processor.set(_: String, _: Map[String, String])).expects(storage, keyValues).never
        put(path, body, headers) {
          status should equal(400)
        }
      }

      it("should set values by keys") {
        testSuccess(keyValues.map(kv => (kv._1, true)), jsonKeyResult)
      }

      it("should return false if the key or value are invalid") {
        testSuccess(Map(someKey -> false), "{\"" + someKey + "\":false}")
      }

      it("should return 400 Bad Request Error if Content-Type is not application/json") {
        testBadRequest(jsonKeyValues, textHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid JSON") {
        testBadRequest("{\"key\"", jsonHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid") {
        testBadRequest("{\"field\":[]}", jsonHeaders)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.set(_: String, _: Map[String, String])).expects(storage, keyValues).returning(internalError).once
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(delete by the key)") {
      val path = s"/delete/$storage/$someKey"

      def test(result: Future[Either[StorageError, Unit]], status: Int) = {
        (processor.delete(_: String, _: String)).expects(storage, someKey).returning(Future(Right(Unit))).once
        delete(path, Seq(), Map()) {
          status should equal(status)
        }
      }

      it("should delete the value by the key") {
        test(Future(Right(Unit)), 200)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        test(internalError, 500)
      }
    }

    describe("(delete by keys)") {
      val path = "/delete/" + storage

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        (processor.delete(_: String, _: Iterable[String])).expects(storage, keys).returning(Future(Right(result))).once
        put(path, jsonKeys, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
        (processor.delete(_: String, _: Iterable[String])).expects(storage, keys).never
        put(path, body, headers) {
          status should equal(400)
        }
      }

      it("should delete values by keys") {
        testSuccess(keyValues.map(kv => (kv._1, true)), jsonKeyResult)
      }

      it("should return false if the key does not exist") {
        testSuccess(Map(someKey -> false), "{\"" + someKey + "\":false}")
      }

      it("should return 400 Bad Request Error if Content-Type is not application/json") {
        testBadRequest(jsonKeys, textHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid JSON") {
        testBadRequest("[\"key\"", jsonHeaders)
      }

      it("should return 400 Bad Request Error if the body is invalid") {
        testBadRequest("{\"key\":null}", jsonHeaders)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.delete(_: String, _: Iterable[String])).expects(storage, keys).returning(internalError).once
        put("/delete/someStorage", jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(list)") {
      val path = "/list/" + storage

      it("should returns keys") {
        (processor.list(_: String)).expects(storage).returning(Future(Right(keys))).once
        get(path, Seq(), Map()) {
          status should equal(200)
          body should equal(jsonKeys)
          response.getContentType should include("application/json")
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        (processor.list(_: String)).expects(storage).returning(internalError).once
        get(path, Seq(), textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(clear)") {
      val path = "/clear/" + storage

      def test(result: Future[Either[StorageError, Unit]], status: Int) = {
        (processor.clear(_: String)).expects(storage).returning(result).once
        put(path, Array[Byte](), Map()) {
          status should equal(status)
        }
      }

      it("should clear the storage") {
        test(Future(Right(Unit)), 200)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        test(internalError, 500)
      }

      it("should return 409 Conflict Error if the document is changed while deletion") {
        test(Future(Left(ConflictError())), 409)
      }
    }
  }
}
