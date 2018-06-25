// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.bwsw.cloudstack.storage.kv.error._
import com.bwsw.cloudstack.storage.kv.mock.MockActor
import com.bwsw.cloudstack.storage.kv.mock.MockActor.Expectation
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.message.request._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest._

import scala.concurrent.Future
import scala.concurrent.duration._

class KvStorageServletSuite
  extends ScalatraSuite
    with FunSpecLike
    with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system: ActorSystem = ActorSystem()
  private val processor = mock[KvProcessor]
  private val kvActor = TestActorRef(new MockActor())
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
    addServlet(new KvStorageServlet(system, 1.second, processor, kvActor), "/*")

    describe("(get by key)") {
      val path = s"/get/$storage/$someKey"

      it("should return the value if the key exists") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvGetRequest(storage, someKey), () => Right(someValue)))
        get(path, Seq(), Map()) {
          status should equal(200)
          body should equal(someValue)
          response.getContentType should include("text/plain")
        }
      }

      it("should return 404 Not Found if key or storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvGetRequest(storage, someKey), () => Left(NotFoundError())))
        get(path, Seq(), Map()) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvGetRequest(storage, someKey), () => internalError))
        get(path, Seq(), Map()) {
          status should equal(500)
        }
      }
    }

    describe("(get by keys)") {
      val path = s"/get/$storage"

      def testSuccess(result: Map[String, Option[String]], expectedBody: String) = {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiGetRequest(storage, keys), () => Right(result)))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
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

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiGetRequest(storage, keys), () => Left(NotFoundError())))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiGetRequest(storage, keys), () => internalError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set the key/value)") {
      val path = s"/set/$storage/$someKey"

      it("should set the value by the key") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvSetRequest(storage, someKey, someValue), () => Right(Unit)))
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
        kvActor.underlyingActor.clearAndExpect(
          Expectation(KvSetRequest(storage, someKey, someValue), () => Left(BadRequestError())))
        put(path, someValue, textHeaders) {
          status should equal(400)
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvSetRequest(storage, someKey, someValue), () => Left(NotFoundError())))
        put(path, someValue, textHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(
          Expectation(KvSetRequest(storage, someKey, someValue), () => internalError))
        put(path, someValue, textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set key/value pairs)") {
      val path = s"/set/$storage"

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiSetRequest(storage, keyValues), () => Right(result)))
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
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

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiSetRequest(storage, keyValues), () => Left(NotFoundError())))
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiSetRequest(storage, keyValues), () => internalError))
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(delete by the key)") {
      val path = s"/delete/$storage/$someKey"

      def test(result: Future[Either[StorageError, Unit]], status: Int) = {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvDeleteRequest(storage, someKey), () => result))
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
      val path = s"/delete/$storage"

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiDeleteRequest(storage, keys), () => Right(result)))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(200)
          body should equal(expectedBody)
          response.getContentType should include("application/json")
        }
      }

      def testBadRequest(body: Array[Byte], headers: scala.Iterable[(String, String)]) = {
        post(path, body, headers) {
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

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiDeleteRequest(storage, keys), () => Left(NotFoundError())))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvMultiDeleteRequest(storage, keys), () => internalError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(list)") {
      val path = s"/list/$storage"

      it("should returns keys") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvListRequest(storage), () => Right(keys)))
        get(path, Seq(), Map()) {
          status should equal(200)
          body should equal(jsonKeys)
          response.getContentType should include("application/json")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvListRequest(storage), () => Left(NotFoundError())))
        get(path, Seq(), textHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvListRequest(storage), () => internalError))
        get(path, Seq(), textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(clear)") {
      val path = s"/clear/$storage"

      def test(result: Future[Either[StorageError, Unit]], status: Int) = {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvClearRequest(storage), () => result))
        post(path, Array[Byte](), Map()) {
          status should equal(status)
        }
      }

      it("should clear the storage") {
        test(Future(Right(Unit)), 200)
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor.clearAndExpect(Expectation(KvListRequest(storage), () => Left(NotFoundError())))
        test(Future(Left(NotFoundError())), 404)
      }

      it("should return 409 Conflict Error if the document is changed while deletion") {
        test(Future(Left(ConflictError())), 409)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        test(internalError, 500)
      }
    }
  }
}
