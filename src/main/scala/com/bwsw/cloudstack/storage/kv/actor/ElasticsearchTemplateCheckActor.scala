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

import akka.actor.ActorLogging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.HEAD
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.request.TemplateCheckRequest
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

/** Actor to check whether the index template exists in Elasticsearch **/
class ElasticsearchTemplateCheckActor(implicit inj: Injector, materializer: ActorMaterializer)
  extends TemplateCheckActor
  with ActorLogging {

  import context.dispatcher

  private val http = Http(context.system)
  private val elasticsearchConfig = inject[ElasticsearchConfig]

  def receive: PartialFunction[Any, Unit] = {
    case TemplateCheckRequest(name, checkName) =>
      val uri = elasticsearchConfig.getUri + "/_template/" + name
      http.singleRequest(HttpRequest(HEAD, uri)).map {
        case resp@HttpResponse(code, _, _, _) =>
          resp.discardEntityBytes()
          code match {
            case StatusCodes.OK => Check(checkName, Healthy, Ok)
            case StatusCodes.NotFound => Check(checkName, Unhealthy, NotFound)
            case unexpected =>
              log.error("Template {} existence check finished unexpectedly: {}", name, unexpected.intValue())
              Check(checkName, Unhealthy, Unexpected("Unexpected status: " + unexpected.intValue()))
          }
      }.recover {
        case ex =>
          log.error(ex, "Template {} existence check failed:", name)
          Check(checkName, Unhealthy, Unexpected(ex.getMessage))
      }.pipeTo(sender())
  }
}
