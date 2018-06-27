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

package com.bwsw.cloudstack.storage.kv.actor

import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.request.{CheckTemplateExistsRequest, HealthCheckRequest}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import scaldi.Injector
import scaldi.akka.AkkaInjectable._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.message.response.HealthCheckResponse
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse

import scala.concurrent.Future

class ElasticsearchHealthActor(implicit inj: Injector) extends HealthActor {

  import context.dispatcher

  private val checkActor = injectActorRef[CheckActor]
  private val client = inject[HttpClient]
  private val appConfig = inject[AppConfig]
  implicit val duration: Timeout = appConfig.getRequestTimeout
  private val registryIndex = "storage-registry"

  def receive: PartialFunction[Any, Unit] = {
    case HealthCheckRequest(detailed) =>
      (for {
        registry <- client.execute(indexExists(registryIndex))
        storageTemplate <- getIndexTemplate("storage")
        historyStorageTemplate <- getIndexTemplate("history-storage")
      } yield HealthCheckResponse(detailed, registry, storageTemplate, historyStorageTemplate)).pipeTo(self)(sender())

    case HealthCheckResponse(true, registry, storageTemplate, historyStorageTemplate) =>
      val checks = Seq(
        indexExistsCheck(StorageRegistry, registry),
        templateExistsCheck(StorageTemplate, storageTemplate),
        templateExistsCheck(HistoryStorageTemplate, historyStorageTemplate)
      )
      val status = if (checks.forall(_.status == Healthy)) Healthy else Unhealthy
      sender() ! HealthCheckDetailedResponseBody(status, checks)
    case HealthCheckResponse(false, registry, storageTemplate, historyStorageTemplate) =>
      sender() ! (registry match {
        case Right(success) =>
          if (success.result.exists && storageTemplate && historyStorageTemplate)
            HealthCheckShortResponseBody(Healthy)
          else
            HealthCheckShortResponseBody(Unhealthy)
        case _ => HealthCheckShortResponseBody(Unhealthy)
      })
  }

  private def getResponses = for {
    registry <- client.execute(indexExists(registryIndex))
    storageTemplate <- getIndexTemplate("storage")
    historyStorageTemplate <- getIndexTemplate("history-storage")
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

  private def templateExistsCheck(name: CheckName, response: Boolean): Check = {
    if (response)
      Check(name, Healthy, "")
    else
      Check(name, Unhealthy, "Not found")
  }

  private def getIndexTemplate(name: String): Future[Boolean] = {
    (checkActor ? CheckTemplateExistsRequest(name)).map {
      case true => true
      case _ => false
    }
  }
}
