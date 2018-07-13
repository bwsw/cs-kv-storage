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

package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.util.elasticsearch._
import com.bwsw.cloudstack.storage.kv.util.test.{getRequestFailureFuture, getRequestSuccessFuture}
import com.sksamuel.elastic4s.get.GetDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.get.GetResponse
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.ExecutionContext

class ElasticsearchStorageLoaderSpec extends AsyncFunSpec with AsyncMockFactory {

  private val storageUuid = "someStorage"
  private val storage = Storage(storageUuid, StorageType.Account, historyEnabled = true)
  private val source = Map(
    RegistryFields.Type -> storage.storageType,
    RegistryFields.HistoryEnabled -> storage.historyEnabled,
    RegistryFields.Deleted -> false
  ).asInstanceOf[Map[String, AnyRef]]

  private val deletedSource = Map(
    RegistryFields.Type -> storage.storageType,
    RegistryFields.HistoryEnabled -> storage.historyEnabled,
    RegistryFields.Deleted -> true
  ).asInstanceOf[Map[String, AnyRef]]

  describe("An ElasticsearchStorageLoader") {

    val fakeClient = mock[HttpClient]
    val loader = new ElasticsearchStorageLoader(fakeClient)

    it("should load value from Elasticsearch") {
      val getResponse = GetResponse(storageUuid, RegistryIndex, DocumentType, 1, found = true, Map.empty, source)
      expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
      loader.load(storageUuid).map {
        case Some(s) => assert(s == storage)
        case None => fail
      }
    }

    it("should return None if the storage is marked as deleted") {
      val getResponse = GetResponse(
        storageUuid,
        RegistryIndex,
        DocumentType,
        1,
        found = true,
        Map.empty,
        deletedSource)
      expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
      loader.load(storageUuid).map {
        case Some(s) => fail
        case None => succeed
      }
    }

    it("should return None if no storage found in Elasticsearch") {
      val getResponse = GetResponse(storageUuid, RegistryIndex, DocumentType, 1, found = false, Map.empty, Map.empty)
      expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
      loader.load(storageUuid).map {
        case Some(s) => fail
        case None => succeed
      }
    }

    it("should fail with RuntimeException if loading from Elasticsearch fails") {
      expectGetRequest(fakeClient).returning(getRequestFailureFuture())
      recoverToSucceededIf[RuntimeException] {
        loader.load(storageUuid)
      }
    }

    it("should fail with RuntimeException if no data provided in response from Elasticsearch") {
      val getResponse = GetResponse(storageUuid, RegistryIndex, DocumentType, 1, found = true, Map.empty, Map.empty)
      expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))
      recoverToSucceededIf[RuntimeException] {
        loader.load(storageUuid)
      }
    }
  }

  private def expectGetRequest(client: HttpClient) = {
    (client
      .execute[GetDefinition, GetResponse]
      (_: GetDefinition)
      (_: HttpExecutable[GetDefinition, GetResponse], _: ExecutionContext))
      .expects(ElasticDsl.get(storageUuid).from(RegistryIndex / DocumentType), GetHttpExecutable, *)
  }
}
