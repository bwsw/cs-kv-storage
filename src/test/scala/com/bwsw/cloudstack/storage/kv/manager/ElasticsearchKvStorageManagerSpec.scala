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

import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, UnauthorizedError}
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.RegistryFields._
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.ScriptOperations.Updated
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{DocumentType, RegistryIndex, StorageType}
import com.bwsw.cloudstack.storage.kv.util.test.{getRequestFailureFuture, getRequestSuccessFuture}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.update.UpdateResponse
import com.sksamuel.elastic4s.http.{update => _, _}
import com.sksamuel.elastic4s.update.UpdateDefinition
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchKvStorageManagerSpec extends AsyncFunSpec with AsyncMockFactory {

  private val cache = mock[StorageCache]
  private val clock = mock[Clock]

  private val storageUuid = "someStorage"
  private val secretKey = "secret".toCharArray
  private val storage = Storage(storageUuid, StorageType.Temporary, historyEnabled = false, secretKey)
  private val ttl = 300000
  private val exception = new RuntimeException("test exception")
  private val timestamp = System.currentTimeMillis()

  describe("An ElasticsearchStorageManager") {
    implicit val fakeClient: HttpClient = mock[HttpClient]
    val manager = new ElasticsearchKvStorageManager(fakeClient, cache, clock)

    describe("(update temp storage ttl)") {

      it("should update the value of ttl field in storage registry") {
        expectExistingStorage
        val updateResponse = UpdateResponse(
          RegistryIndex,
          DocumentType,
          storageUuid,
          2,
          Updated,
          forcedRefresh = false,
          Shards(2, 1, 0),
          None)
        expectUpdateRequest(fakeClient).returning(getRequestSuccessFuture(updateResponse))
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Right(_) => succeed
          case _ => fail
        }
      }

      it("should return BadRequestError if type of the storage isn't TEMP") {
        expectNotTemporaryStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if there is no such storage") {
        expectNotFoundStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if update request failed with 404 NotFound") {
        expectExistingStorage
        expectUpdateRequest(fakeClient).returning(getRequestFailureFuture(404))
        expectStorageDeletion
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if storage retrieval fails") {
        expectStorageCacheFailure
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if the request fails") {
        expectExistingStorage
        expectUpdateRequest.returning(getRequestFailureFuture())

        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return UnauthorizedError if invalid secret key is provided") {
        expectDifferentSecretKeyStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.updateTempStorageTtl(storageUuid, secretKey, ttl).map {
          case Left(_: UnauthorizedError) => succeed
          case _ => fail
        }
      }
    }

    describe("(delete temp storage)") {

      it("should mark the storage as deleted in a registry") {
        expectExistingStorage
        val updateResponse = UpdateResponse(
          RegistryIndex,
          DocumentType,
          storageUuid,
          2,
          Updated,
          forcedRefresh = false,
          Shards(2, 1, 0),
          None)
        expectDeleteRequest.returning(getRequestSuccessFuture(updateResponse))
        expectStorageDeletion

        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Right(_) => succeed
          case _ => fail
        }
      }

      it("should return BadRequestError if type of the storage isn't TEMP") {
        expectNotTemporaryStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if there is no such storage in cache") {
        expectNotFoundStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return NotFoundError if update request failed with 404 NotFound") {
        expectExistingStorage
        expectDeleteRequest.returning(getRequestFailureFuture(404))
        expectStorageDeletion
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if the request fails") {
        expectExistingStorage
        expectDeleteRequest.returning(getRequestFailureFuture())

        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if storage retrieval failed") {
        expectStorageCacheFailure
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return UnauthorizedError if invalid secret key is provided") {
        expectDifferentSecretKeyStorage
        (clock.currentTimeMillis _).expects().returning(timestamp)
        manager.deleteTempStorage(storageUuid, secretKey).map {
          case Left(_: UnauthorizedError) => succeed
          case _ => fail
        }
      }
    }
  }

  private def expectUpdateRequest(implicit client: HttpClient) = {
    (client.execute[UpdateDefinition, UpdateResponse](_: UpdateDefinition)(
      _: HttpExecutable[UpdateDefinition, UpdateResponse],
      _: ExecutionContext))
      .expects(
        update(storageUuid) in RegistryIndex / DocumentType script
          s"ctx._source.$ExpirationTimestamp = ctx._source" +
            s".$ExpirationTimestamp - ctx._source.$Ttl + $ttl; ctx._source.$Ttl = $ttl; " +
            s"ctx._source.$LastUpdated = $timestamp",
        UpdateHttpExecutable,
        *)
  }

  private def expectDeleteRequest(implicit client: HttpClient) = {
    (client.execute[UpdateDefinition, UpdateResponse](_: UpdateDefinition)(
      _: HttpExecutable[UpdateDefinition, UpdateResponse],
      _: ExecutionContext))
      .expects(
        update(storageUuid) in RegistryIndex / DocumentType doc (Deleted -> true, LastUpdated -> timestamp),
        UpdateHttpExecutable,
        *)
  }

  private def expectExistingStorage = {
    (cache.get _).expects(storageUuid).returning(Future(Some(storage)))
  }

  private def expectNotTemporaryStorage = {
    (cache.get _).expects(storageUuid).returning(Future(Some(storage.copy(storageType = StorageType.Account))))
  }

  private def expectNotFoundStorage = {
    (cache.get _).expects(storageUuid).returning(Future(None))
  }

  private def expectStorageCacheFailure = {
    (cache.get _).expects(storageUuid).returning(Future.failed(exception))
  }

  private def expectDifferentSecretKeyStorage = {
    (cache.get _).expects(storageUuid).returning(Future(Some(storage.copy(secretKey = "anotherSecret".toCharArray))))
  }

  private def expectStorageDeletion = {
    (cache.delete _).expects(storageUuid)
  }
}
