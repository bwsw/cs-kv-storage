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

import com.bwsw.cloudstack.storage.kv.entity._
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.index.GetIndexTemplates
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}

import scala.concurrent.Future

class ElasticsearchHealthProcessor(client: HttpClient) extends HealthProcessor {
  private val registryIndex = "storage-registry"

  import scala.concurrent.ExecutionContext.Implicits.global

  def check: Future[HealthStatus] = {
    getResponses.map {
      case (Right(registry), Right(storageTemplate), Right(historyStorageTemplate)) =>
        if (registry.result.exists && storageTemplate.result.templates.nonEmpty && historyStorageTemplate.result.templates.nonEmpty)
          Healthy
        else
          Unhealthy
      case _ => Unhealthy
    }
  }

  def checkDetailed: Future[HealthResponseBody] = {
    getResponses.map {
      case (registry, storageTemplate, historyStorageTemplate) =>
        val checks = Seq(
          indexExistsCheck(StorageRegistry, registry),
          templateExistsCheck(StorageTemplate, storageTemplate),
          templateExistsCheck(HistoryStorageTemplate, historyStorageTemplate)
        )
        val status = if (checks.forall(_.status == Healthy)) Healthy else Unhealthy
        HealthResponseBody(status, checks)
    }
  }

  private def getResponses = for {
    registry <- client.execute(indexExists(registryIndex))
    storageTemplate <- client.execute(getIndexTemplate("storage"))
    historyStorageTemplate <- client.execute(getIndexTemplate("history-storage"))
  } yield (registry, storageTemplate, historyStorageTemplate)

  private def indexExistsCheck(name: CheckName, response: Either[RequestFailure, RequestSuccess[IndexExistsResponse]]): Check = {
    response match {
      case Left(failure) =>
        val message = if (failure.error == null) "Elasticsearch error" else failure.error.reason
        Check(name, Unhealthy, message)
      case Right(success) =>
        val status = if (success.result.exists) Healthy else Unhealthy
        val message = if (success.result.exists) "" else "Not found"
        Check(name, status, message)
    }
  }

  private def templateExistsCheck(name: CheckName, response: Either[RequestFailure, RequestSuccess[GetIndexTemplates]]): Check = {
    response match {
      case Left(failure) =>
        val message = if (failure.error == null) "Elasticsearch error" else failure.error.reason
        Check(name, Unhealthy, message)
      case Right(success) =>
        val status = if (success.result.templates.nonEmpty) Healthy else Unhealthy
        val message = if (success.result.templates.nonEmpty) "" else "Not found"
        Check(name, status, message)
    }
  }
}
