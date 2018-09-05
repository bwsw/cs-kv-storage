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
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, UnauthorizedError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import com.bwsw.cloudstack.storage.kv.message.response.KvHistoryResponse
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.StorageType.Temporary
import scaldi.Injector
import scaldi.akka.AkkaInjectable.inject

import scala.concurrent.Future

/** Actor responsible for history retrieval with pagination/scrolling **/
class DefaultHistoryRequestActor(implicit inj: Injector)
  extends HistoryRequestActor
  with akka.actor.ActorLogging {

  import context.dispatcher

  private val historyProcessor = inject[HistoryProcessor]
  private val storageCache = inject[StorageCache]

  override def receive: Receive = {
    case request: KvHistoryGetRequest =>
      storageCache.get(request.storageUuid).flatMap {
        case Some(storage) =>
          if (request.secretKey == storage.secretKey) {
            if (storage.historyEnabled && storage.storageType != Temporary) {
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
            } else {
              Future(Left(BadRequestError()))
            }
          } else {
            Future(Left(UnauthorizedError()))
          }
        case None =>
          Future(Left(NotFoundError()))
      }.map(result => KvHistoryResponse(result)).pipeTo(self)(sender())
    case request: KvHistoryScrollRequest =>
      historyProcessor.scroll(request.scrollId, request.timeout).map(result => KvHistoryResponse(result)).pipeTo(self)(
        sender())
    case KvHistoryResponse(response) =>
      sender() ! response
    case failure: Status.Failure =>
      log.error(failure.cause, getClass.getName)
      sender() ! Left(InternalError(failure.cause.getMessage))
  }
}
