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
import akka.pattern.pipe
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.entity.Operation.{Clear, Delete, Set}
import com.bwsw.cloudstack.storage.kv.entity.{Operation, Storage}
import com.bwsw.cloudstack.storage.kv.error.{InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.message.response._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.util.Clock
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.concurrent.Future

/** Actor handling users requests and logging history of changes **/
class HistoricalKvActor(implicit inj: Injector)
  extends KvActor
  with ActorLogging {

  import context.dispatcher

  private val clock = inject[Clock]
  private val historyKvActor = injectActorRef[HistoryKvActor]
  private val kvProcessor = inject[KvProcessor]
  private val storageCache = inject[StorageCache]

  override def receive: Receive = {
    case request: KvGetRequest =>
      process(request, (request: KvGetRequest, storage: Storage) => {
        kvProcessor.get(request.storage, request.key).map(r => KvGetResponse(r))
      })
    case KvGetResponse(response) =>
      sender() ! response
    case request: KvMultiGetRequest =>
      process(request, (request: KvMultiGetRequest, storage: Storage) => {
        kvProcessor.get(request.storage, request.keys).map(r => KvMultiGetResponse(r))
      })
    case KvMultiGetResponse(response) =>
      sender() ! response
    case request: KvSetRequest =>
      val timestamp = clock.currentTimeMillis
      process(request, (request: KvSetRequest, storage: Storage) => {
        kvProcessor.set(request.storage, request.key, request.value)
          .map(r => KvSetResponse(storage, request.key, request.value, timestamp, r))
      })
    case KvSetResponse(storage, key, value, timestamp, response) =>
      logHistory(response, storage, key, value, timestamp, Set)
      sender() ! response
    case request: KvMultiSetRequest =>
      val timestamp = clock.currentTimeMillis
      process(request, (request: KvMultiSetRequest, storage: Storage) => {
        kvProcessor.set(request.storage, request.kvs).map(r => KvMultiSetResponse(storage, request.kvs, timestamp, r))
      })
    case KvMultiSetResponse(storage, kvs, timestamp, response) =>
      logHistory(response, storage, timestamp, Set, kvs)
      sender() ! response
    case request: KvDeleteRequest =>
      val timestamp = clock.currentTimeMillis
      process(request, (request: KvDeleteRequest, storage: Storage) => {
        kvProcessor.delete(request.storage, request.key).map(r => KvDeleteResponse(storage, request.key, timestamp, r))
      })
    case KvDeleteResponse(storage, key, timestamp, response) =>
      logHistory(response, storage, key, null, timestamp, Delete)
      sender() ! response
    case request: KvMultiDeleteRequest =>
      val timestamp = clock.currentTimeMillis
      process(request, (request: KvMultiDeleteRequest, storage: Storage) => {
        kvProcessor.delete(request.storage, request.keys).map(r => KvMultiDeleteResponse(storage, timestamp, r))
      })
    case KvMultiDeleteResponse(storage, timestamp, response) =>
      logHistory(response, storage, timestamp, Delete)
      sender() ! response
    case request: KvListRequest =>
      process(request, (request: KvListRequest, storage: Storage) => {
        kvProcessor.list(request.storage).map(r => KvListResponse(r))
      })
    case KvListResponse(response) =>
      sender() ! response
    case request: KvClearRequest =>
      val timestamp = clock.currentTimeMillis
      process(request, (request: KvClearRequest, storage: Storage) => {
        kvProcessor.clear(request.storage).map(r => KvClearResponse(storage, timestamp, r))
      })
    case KvClearResponse(storage, timestamp, response) =>
      logHistory(response, storage, null, null, timestamp, Clear)
      sender() ! response
    case KvErrorResponse(error) =>
      sender() ! Left(error)
    case failure: Status.Failure =>
      log.error(failure.cause, getClass.getName)
      sender() ! Left(InternalError(failure.cause.getMessage))
  }

  protected def process[S <: KvRequest](r: S, producer: (S, Storage) => Future[KvResponse]): Future[KvResponse] = {
    storageCache.get(r.storage).flatMap {
      case Some(storage) => producer.apply(r, storage)
      case None => Future(KvErrorResponse(NotFoundError()))
    }.pipeTo(self)(sender())
  }

  protected def logHistory(
      response: Either[StorageError, Unit],
      storage: Storage,
      key: String,
      value: String,
      timestamp: Long,
      operation: Operation): Unit = {
    if (storage.historyEnabled)
      response match {
        case Right(_) =>
          historyKvActor ! KvHistory(storage.uUID, key, value, timestamp, operation)
        case Left(_) => // do nothing
      }
  }

  protected def logHistory(
      response: Either[StorageError, Map[String, Boolean]],
      storage: Storage,
      timestamp: Long,
      operation: Operation,
      values: Map[String, String] = Map()): Unit = {
    if (storage.historyEnabled)
      response match {
        case Right(results) =>
          historyKvActor ! KvHistoryBulk(results.filter(_._2).map {
            case (key, _) => KvHistory(storage.uUID, key, values.getOrElse(key, null), timestamp, operation)
          })
        case Left(_) => // do nothing
      }
  }
}
