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

import akka.actor.Status
import akka.pattern.pipe
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import com.bwsw.cloudstack.storage.kv.message.response.KvHistoryResponse
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable.inject

import scala.concurrent.Future

/** Actor responsible for page-by-page retrieving of history records **/
class PagedHistoryRequestActor(implicit inj: Injector)
  extends HistoryRequestActor
  with akka.actor.ActorLogging {

  import context.dispatcher

  private val historyProcessor = inject[HistoryProcessor]
  private val storageCache = inject[StorageCache]

  override def receive: Receive = {
    case request: KvHistoryGetRequest =>
      storageCache.isHistoryEnabled(request.storageUuid).flatMap {
        case Some(true) =>
          historyProcessor.get(
            request.storageUuid,
            request.keys,
            request.operations,
            request.start,
            request.end,
            request.sort,
            request.page,
            request.size,
            request.scroll)
        case Some(false) =>
          Future(Left(BadRequestError()))
        case None =>
          Future(Left(NotFoundError()))
      }.map(body => KvHistoryResponse(body)).pipeTo(self)(sender())
    case request: KvHistoryScrollRequest =>
      historyProcessor.scroll(request.scrollId, request.timeout).map(body => KvHistoryResponse(body)).pipeTo(self)(
        sender())
    case KvHistoryResponse(body) =>
      sender() ! body
    case failure: Status.Failure =>
      log.error(getClass + ": " + failure.cause.getMessage)
      sender() ! Left(InternalError(failure.cause.getMessage))
  }
}
