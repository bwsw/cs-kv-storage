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

import akka.actor.{ActorLogging, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.CheckName.{HistoryStorageTemplate, StorageRegistry, StorageTemplate}
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.request.{HealthCheckRequest, TemplateCheckRequest}
import com.bwsw.cloudstack.storage.kv.message.response.{DetailedHealthCheckResponse, StatusHealthCheckResponse}
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{DataStorageTemplateName, HistoryStorageTemplateName,
  RegistryIndex}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.concurrent.Future

/** Actor responsible for health checks for Elasticsearch storages **/
class ElasticsearchHealthActor(implicit inj: Injector)
  extends HealthActor
  with ActorLogging {

  import context.dispatcher

  private val checkActor = injectActorRef[TemplateCheckActor]
  private val client = inject[HttpClient]
  private val appConfig = inject[AppConfig]

  implicit val duration: Timeout = appConfig.getRequestTimeout

  def receive: PartialFunction[Any, Unit] = {
    case HealthCheckRequest(true) =>
      (for {
        registry <- checkIndex(RegistryIndex, StorageRegistry)
        storageTemplate <- checkIndexTemplate(DataStorageTemplateName, StorageTemplate)
        historyStorageTemplate <- checkIndexTemplate(HistoryStorageTemplateName, HistoryStorageTemplate)
      } yield HealthCheckRawResponse(Seq(registry, storageTemplate, historyStorageTemplate))).pipeTo(self)(sender())

    case HealthCheckRequest(false) =>
      sender() ! StatusHealthCheckResponse(Healthy)

    case HealthCheckRawResponse(checks) =>
      val status = if (checks.forall(_.status == Healthy)) Healthy else Unhealthy
      sender() ! DetailedHealthCheckResponse(status, checks)

    case failure: Status.Failure =>
      log.error(failure.cause, getClass.getName)
      sender() ! StatusHealthCheckResponse(Unhealthy)
  }

  private def checkIndex(index: String, name: CheckName): Future[Check] = {
    client.execute(indexExists(index)).map {
      case Left(failure) =>
        log.error("Elasticsearch index exists request failure: {}", failure.error)
        Check(
          name,
          Unhealthy,
          if (failure.error == null) ElasticsearchError() else ElasticsearchError(failure.error.reason))
      case Right(success) =>
        if (success.result.exists)
          Check(name, Healthy, Ok)
        else
          Check(name, Unhealthy, NotFound)
    }.recover {
      case ex =>
        log.error(ex, "Index {} existence check failed:", index)
        Check(name, Unhealthy, Unexpected(ex.getMessage))
    }
  }

  private def checkIndexTemplate(name: String, checkName: CheckName): Future[Check] = {
    (checkActor ? TemplateCheckRequest(name, checkName)).map {
      case check: Check => check
    }
  }

  private case class HealthCheckRawResponse(checks: Iterable[Check])

}
