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
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.manager.KvStorageManager
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.SecretKeyHeader
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.Future

class KvStorageManagerServletSuite
  extends ScalatraSuite
  with FunSpecLike
  with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val system = ActorSystem()
  private val manager = mock[KvStorageManager]
  private val storageUuid = "somestorage"
  private val secretKey = "secret".toCharArray
  private val headers = Map(SecretKeyHeader -> secretKey.mkString)
  private val path = "/storage/" + storageUuid
  private val ttl = 300000
  private val ttlParams = Seq(("ttl", ttl.toString))
  private val badTtlParams = Seq(("ttl", "badTTL"))
  private val message = "test error"

  describe("a KvStorageManagerServlet") {
    addServlet(new KvStorageManagerServlet(system, manager), "/storage/*")

    describe("(update temp storage ttl)") {
      def testProvidesError(error: StorageError, status: Int) = {
        (manager.updateTempStorageTtl(_: String, _: Array[Char], _: Long)).expects(storageUuid, *, ttl)
          .returning(Future(Left(error))).once

        put(path, ttlParams, headers) {
          status should equal(status)
        }
      }

      it("should update the value of ttl field in storage registry") {
        (manager.updateTempStorageTtl(_: String, _: Array[Char], _: Long)).expects(storageUuid, *, ttl)
          .returning(Future(Right(()))).once
        put(path, ttlParams, headers) {
          status should equal(200)
        }
      }

      it("should return 400 BadRequest if no ttl is specified") {
        (manager.updateTempStorageTtl(_: String, _: Array[Char], _: Long)).expects(storageUuid, *, ttl).never
        put(path, Seq(), headers) {
          status should equal(400)
        }
      }

      it("should return 400 BadRequest if ttl can not be converted to Long") {
        (manager.updateTempStorageTtl(_: String, _: Array[Char], _: Long)).expects(storageUuid, *, ttl).never
        put(path, badTtlParams, headers) {
          status should equal(400)
        }
      }

      it("should return 400 BadRequest if the manager returns BadRequestError") {
        testProvidesError(BadRequestError(), 400)
      }

      it("should return 404 NotFound if the manager returns NotFoundError") {
        testProvidesError(NotFoundError(), 404)
      }

      it("should return 500 InternalServerError if the manager returns InternalError") {
        testProvidesError(InternalError(message), 500)
      }

      it("should return 401 Unauthorized if no Secret-Key header provided") {
        put(path, ttlParams) {
          status should equal(401)
        }
      }
    }
  }

  describe("(delete temp storage)") {
    def testProvidesError(error: StorageError, status: Int) = {
      (manager.deleteTempStorage(_: String, _: Array[Char])).expects(storageUuid, *)
        .returning(Future(Left(error))).once

      delete(path, Seq(), headers) {
        status should equal(status)
      }
    }

    it("should delete the storage") {
      (manager.deleteTempStorage(_: String, _: Array[Char])).expects(storageUuid, *)
        .returning(Future(Right(()))).once
      delete(path, Seq(), headers) {
        status should equal(200)
      }
    }

    it("should return 400 BadRequest if the manager returns BadRequestError") {
      testProvidesError(BadRequestError(), 400)
    }

    it("should return 404 NotFound if the manager returns NotFoundError") {
      testProvidesError(NotFoundError(), 404)
    }

    it("should return 500 InternalServerError if the manager returns InternalError") {
      testProvidesError(InternalError(message), 500)
    }

    it("should return 401 Unauthorized if no Secret-Key header provided") {
      delete(path) {
        status should equal(401)
      }
    }
  }
}
