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

package com.bwsw.cloudstack.storage.kv.manager

import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.update.UpdateResponse
import com.sksamuel.elastic4s.http.{update => _, _}
import com.sksamuel.elastic4s.update.UpdateDefinition
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchKvStorageManagerSpec extends AsyncFunSpec with AsyncMockFactory {

  private val storage = "someStorage"
  private val ttl = 300000
  private val registry = "storage-registry"
  private val `type` = "_doc"
  private val tempType = "TEMP"

  describe("An ElasticsearchStorageManager") {
    implicit val fakeClient: HttpClient = mock[HttpClient]
    val manager = new ElasticsearchKvStorageManager(fakeClient)

    describe("(update temp storage ttl)") {
      it("should update the value of ttl field in storage registry") {
        val updateResponse = UpdateResponse(registry, `type`, storage, 2, "updated", forcedRefresh = false, Shards(2, 1, 0), None)
        expectUpdateRequest.returning(getRequestSuccessFuture(updateResponse))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Right(_) => succeed
          case _ => fail
        }
      }

      it("should return BadRequestError if type of the storage isn't TEMP") {
        val updateResponse = UpdateResponse(registry, `type`, storage, 2, "noop", forcedRefresh = false, Shards(2, 1, 0), None)
        expectUpdateRequest.returning(getRequestSuccessFuture(updateResponse))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if there is no such storage") {
        expectUpdateRequest.returning(getRequestFailureFuture(404))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if the request fails") {
        expectUpdateRequest.returning(getRequestFailureFuture(500))
        manager.updateTempStorageTtl(storage, ttl).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(delete temp storage)") {

      it("should mark the storage 'deleted' in a registry") {
        val updateResponse = UpdateResponse(
          registry,
          `type`,
          storage,
          2,
          "updated",
          forcedRefresh = false,
          Shards(2, 1, 0),
          None)
        expectDeleteRequest.returning(getRequestSuccessFuture(updateResponse))

        manager.deleteTempStorage(storage).map {
          case Right(_) => succeed
          case _ => fail
        }
      }

      it("should return BadRequestError if type of the storage isn't TEMP") {
        val updateResponse = UpdateResponse(
          registry,
          `type`,
          storage,
          2,
          "noop",
          forcedRefresh = false,
          Shards(2, 1, 0),
          None)
        expectDeleteRequest.returning(getRequestSuccessFuture(updateResponse))
        manager.deleteTempStorage(storage).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if there is no such storage in registry") {
        expectDeleteRequest.returning(getRequestFailureFuture(404))
        manager.deleteTempStorage(storage).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if update request fails") {
        expectDeleteRequest.returning(getRequestFailureFuture(500))
        manager.deleteTempStorage(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }


      it("should return InternalError if delete index request fails") {
        expectDeleteRequest.returning(getRequestFailureFuture(500))
        manager.deleteTempStorage(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

    }
  }

  private def expectUpdateRequest(implicit client: HttpClient) = {
    (client.execute[UpdateDefinition, UpdateResponse](_: UpdateDefinition)(_: HttpExecutable[UpdateDefinition, UpdateResponse], _: ExecutionContext))
      .expects(update(storage) in registry / `type`
        script "if (ctx._source.type == \"" + tempType + "\"){ ctx._source.ttl = " + ttl + " } else { ctx.op=\"noop\"}",
        UpdateHttpExecutable,
        *)
  }

  private def expectDeleteRequest(implicit client: HttpClient) = {
    (client.execute[UpdateDefinition, UpdateResponse](_: UpdateDefinition)(
      _: HttpExecutable[UpdateDefinition, UpdateResponse],
      _: ExecutionContext))
      .expects(
        update(storage) in registry / `type`
          script "if (ctx._source.type == \"" + tempType + "\"){ ctx._source.deleted = true } else { ctx.op=\"noop\"}",
        UpdateHttpExecutable,
        *)
  }

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }

  private def getRequestFailureFuture[T](status: Int): Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(status, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }
}
