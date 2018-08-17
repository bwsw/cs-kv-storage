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
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.mock.MockActor
import com.bwsw.cloudstack.storage.kv.mock.MockActor.ResponsiveExpectation
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.SecretKeyHeader
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest._

import scala.concurrent.duration._

class KvStorageServletSuite extends ScalatraSuite with FunSpecLike with MockFactory {

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
  private val storageUuid = "someStorage"
  private val secretKey = "secret".toCharArray
  private val internalError = Left(InternalError("some reason"))
  private val notFoundError = Left(NotFoundError())
  private val commonHeaders = Map(SecretKeyHeader -> secretKey.mkString)
  private val jsonHeaders = Map("Content-Type" -> "application/json") ++ commonHeaders
  private val textHeaders = Map("Content-Type" -> "text/plain") ++ commonHeaders

  describe("a KvStorageServlet") {
    addServlet(new KvStorageServlet(system, 1.second, processor, kvActor), "/*")

    describe("(get by key)") {
      val path = s"/get/$storageUuid/$someKey"

      it("should return the value if the key exists") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvGetRequest(storageUuid, secretKey, someKey), () => Right(someValue)))
        get(path, Seq(), commonHeaders) {
          status should equal(200)
          body should equal(someValue)
          response.getContentType should include("text/plain")
        }
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        get(path, Seq(), Map()) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if key or storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvGetRequest(storageUuid, secretKey, someKey), () => notFoundError))
        get(path, Seq(), commonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvGetRequest(storageUuid, secretKey, someKey), () => internalError))
        get(path, Seq(), commonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(get by keys)") {
      val path = s"/get/$storageUuid"

      def testSuccess(result: Map[String, Option[String]], expectedBody: String) = {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvMultiGetRequest(storageUuid, secretKey, keys), () => Right(result)))
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

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        post(path, jsonKeys, jsonHeaders - SecretKeyHeader) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvMultiGetRequest(storageUuid, secretKey, keys), () => notFoundError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvMultiGetRequest(storageUuid, secretKey, keys), () => internalError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set the key/value)") {
      val path = s"/set/$storageUuid/$someKey"

      it("should set the value by the key") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvSetRequest(storageUuid, secretKey, someKey, someValue),
            () => Right(())))
        put(path, someValue, textHeaders) {
          status should equal(200)
          response.getContentType should include("text/plain")
        }
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        put(path, someValue, textHeaders - SecretKeyHeader) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 400 Bad Request Error if Content-Type is not text/plain") {
        put(path, someValue, jsonHeaders) {
          status should equal(400)
        }
      }

      it("should return 400 Bad Request Error if the key or value are invalid") {
        kvActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          KvSetRequest(storageUuid, secretKey, someKey, someValue),
          () => Left(BadRequestError())))
        put(path, someValue, textHeaders) {
          status should equal(400)
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvSetRequest(storageUuid, secretKey, someKey, someValue),
            () => notFoundError))
        put(path, someValue, textHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvSetRequest(storageUuid, secretKey, someKey, someValue),
            () => internalError))
        put(path, someValue, textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(set key/value pairs)") {
      val path = s"/set/$storageUuid"

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiSetRequest(storageUuid, secretKey, keyValues),
            () => Right(result)))
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

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        put(path, jsonKeyValues, jsonHeaders - SecretKeyHeader) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiSetRequest(storageUuid, secretKey, keyValues),
            () => notFoundError))
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiSetRequest(storageUuid, secretKey, keyValues),
            () => internalError))
        put(path, jsonKeyValues, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(delete by the key)") {
      val path = s"/delete/$storageUuid/$someKey"

      def test(result: Either[StorageError, Unit], statusCode: Int) = {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvDeleteRequest(storageUuid, secretKey, someKey), () => result))
        delete(path, Seq(), commonHeaders) {
          status should equal(statusCode)
        }
      }

      it("should delete the value by the key") {
        test(Right(()), 200)
      }

      it("should return 404 Not Found if storage does not exist") {
        test(notFoundError, 404)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        test(internalError, 500)
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        delete(path, Seq(), Map()) {
          status should equal(401)
          body should equal("")
        }
      }
    }

    describe("(delete by keys)") {
      val path = s"/delete/$storageUuid"

      def testSuccess(result: Map[String, Boolean], expectedBody: String) = {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiDeleteRequest(storageUuid, secretKey, keys),
            () => Right(result)))
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

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        post(path, jsonKeys, jsonHeaders - SecretKeyHeader) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiDeleteRequest(storageUuid, secretKey, keys),
            () => notFoundError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvMultiDeleteRequest(storageUuid, secretKey, keys),
            () => internalError))
        post(path, jsonKeys, jsonHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(list)") {
      val path = s"/list/$storageUuid"

      it("should returns keys") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvListRequest(storageUuid, secretKey), () => Right(keys)))
        get(path, Seq(), commonHeaders) {
          status should equal(200)
          body should equal(jsonKeys)
          response.getContentType should include("application/json")
        }
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        get(path, Seq(), Map()) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvListRequest(storageUuid, secretKey), () => notFoundError))
        get(path, Seq(), textHeaders) {
          status should equal(404)
        }
      }

      it("should return 500 Internal Server Error if request processing fails") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvListRequest(storageUuid, secretKey), () => internalError))
        get(path, Seq(), textHeaders) {
          status should equal(500)
        }
      }
    }

    describe("(clear)") {
      val path = s"/clear/$storageUuid"

      def test(result: Either[StorageError, Unit], statusCode: Int) = {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvClearRequest(storageUuid, secretKey), () => result))
        post(path, Seq(), commonHeaders) {
          status should equal(statusCode)
        }
      }

      it("should clear the storage") {
        test(Right(()), 200)
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        post(path, Seq(), Map()) {
          status should equal(401)
          body should equal("")
        }
      }

      it("should return 404 Not Found if storage does not exist") {
        kvActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvClearRequest(storageUuid, secretKey), () => notFoundError))
        test(notFoundError, 404)
      }

      it("should return 409 Conflict Error if the document is changed while deletion") {
        test(Left(ConflictError()), 409)
      }

      it("should return 500 Internal Server Error if request processing fails") {
        test(internalError, 500)
      }
    }
  }
}
