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
import akka.pattern.pipe
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.HEAD
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.message.request.CheckTemplateExistsRequest
import com.bwsw.cloudstack.storage.kv.message.response.CheckTemplateExistsResponse
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

/** Performs checks under Elasticsearch **/
class ElasticsearchCheckActor(implicit inj: Injector)
  extends CheckActor
  with ActorLogging {

  private val http = Http(context.system)
  private val elasticsearchConfig = inject[ElasticsearchConfig]

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  import context.dispatcher

  def receive: PartialFunction[Any, Unit] = {
    case CheckTemplateExistsRequest(name) =>
      val uri = elasticsearchConfig.getUri + "/_template/" + name
      http.singleRequest(HttpRequest(HEAD, uri))
        //        .map(_ => CheckTemplateExistsResponse(_))
        .pipeTo(self)(sender())
    case CheckTemplateExistsResponse(httpResponse) =>
      val answer = httpResponse.status.intValue() match {
        case 200 => true
        case 404 => false
        case _ => false
      }
      httpResponse.discardEntityBytes()
      sender() ! answer

    case resp@HttpResponse(code, _, _, _) =>
      val answer = code match {
        case StatusCodes.OK => true
        case StatusCodes.NotFound => false
        case _ => false
      }
      resp.discardEntityBytes()
      sender() ! answer
  }
}
