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

package com.bwsw.cloudstack.storage.kv.processor

import com.sksamuel.elastic4s.admin.IndicesExists
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse
import com.sksamuel.elastic4s.http.index.{GetIndexTemplates, IndexTemplate}
import com.sksamuel.elastic4s.indexes.GetIndexTemplateDefinition
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec
import com.bwsw.cloudstack.storage.kv.error.InternalError

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHealthProcessorSuite extends AsyncFunSpec with AsyncMockFactory {
  private val registryIndex = "storage-registry"
  describe("a HealthChecker") {
    val fakeClient = mock[HttpClient]
    implicit val client: HttpClient = fakeClient
    val healthProcessor = new ElasticsearchHealthProcessor(fakeClient)

    it("should return true if history template, storage template and storage registry exists") {
      expectStorageTemplateExists
      expectHistoryTemplateExists
      expectStorageRegistryExists
      healthProcessor.check.map {
        case Right(true) => succeed
        case _ => fail
      }
    }

    it("should return false if storage registry doesn't exist") {
      expectStorageTemplateExists
      expectHistoryTemplateExists
      expectIndexExistsRequest.returning(getRequestSuccessFuture(IndexExistsResponse(false)))

      healthProcessor.check.map {
        case Right(false) => succeed
        case _ => fail
      }
    }

    it("should return false if storage template doesn't exist") {
      expectGetIndexTemplateDefinition("storage")
        .returning(getRequestSuccessFuture(GetIndexTemplates(Map.empty)))
      expectHistoryTemplateExists
      expectStorageRegistryExists

      healthProcessor.check.map {
        case Right(false) => succeed
        case _ => fail
      }
    }

    it("should return false if history template doesn't exist") {
      expectStorageTemplateExists
      expectGetIndexTemplateDefinition("history-storage")
        .returning(getRequestSuccessFuture(GetIndexTemplates(Map.empty)))
      expectStorageRegistryExists

      healthProcessor.check.map {
        case Right(false) => succeed
        case _ => fail
      }
    }

    it("should return InternalError if storage registry check failed") {
      expectStorageTemplateExists
      expectHistoryTemplateExists
      expectIndexExistsRequest.returning(getRequestFailureFuture)

      healthProcessor.check.map {
        case Left(_: InternalError) => succeed
        case _ => fail
      }
    }

    it("should return InternalError if storage template check failed") {
      expectGetIndexTemplateDefinition("storage")
        .returning(getRequestFailureFuture)
      expectHistoryTemplateExists
      expectStorageRegistryExists

      healthProcessor.check.map {
        case Left(_: InternalError) => succeed
        case _ => fail
      }
    }

    it("should return InternalError if history template check failed") {
      expectStorageTemplateExists
      expectGetIndexTemplateDefinition("history-storage")
        .returning(getRequestFailureFuture)
      expectStorageRegistryExists

      healthProcessor.check.map {
        case Left(_: InternalError) => succeed
        case _ => fail
      }
    }
  }

  private def expectStorageTemplateExists(implicit client: HttpClient) = expectGetIndexTemplateDefinition("storage")
    .returning(getRequestSuccessFuture(GetIndexTemplates(Map("storage" ->
      IndexTemplate(1, Seq("storage-*"), Map("number_of_shards" -> "1", "number_of_replicas" -> "2"),
        Map("_doc" -> Map("properties" -> Map("value" -> Map("type" -> "keyword", "index" -> false)))), Map.empty)))))

  private def expectHistoryTemplateExists(implicit client: HttpClient) = expectGetIndexTemplateDefinition("history-storage")
    .returning(getRequestSuccessFuture(GetIndexTemplates(Map("history-storage" ->
      IndexTemplate(1, Seq("history-storage-*"), Map("number_of_shards" -> "1", "number_of_replicas" -> "2"), Map(
        "_doc" -> Map("properties" -> Map(
          "value" -> Map("type" -> "keyword", "index" -> false),
          "key" -> Map("type" -> "keyword"),
          "timestamp" -> Map("type" -> "long"),
          "operation" -> Map("type" -> "keyword")
        ))), Map.empty)))))

  private def expectStorageRegistryExists(implicit client: HttpClient) = expectIndexExistsRequest.returning(getRequestSuccessFuture(IndexExistsResponse(true)))

  private def expectGetIndexTemplateDefinition(templateName: String)(implicit client: HttpClient) =
    (client.execute[GetIndexTemplateDefinition, GetIndexTemplates](_: GetIndexTemplateDefinition)(_: HttpExecutable[GetIndexTemplateDefinition, GetIndexTemplates], _: ExecutionContext))
      .expects(getIndexTemplate(templateName), GetIndexTemplateHttpExecutable, *)

  private def expectIndexExistsRequest(implicit client: HttpClient) =
    (client.execute[IndicesExists, IndexExistsResponse](_: IndicesExists)(_: HttpExecutable[IndicesExists, IndexExistsResponse], _: ExecutionContext))
      .expects(indexExists(registryIndex), IndexExistsHttpExecutable, *)

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }

  private def getRequestFailureFuture[T]: Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }
}
