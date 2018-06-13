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
import com.bwsw.cloudstack.storage.kv.error.{InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.message.response._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.concurrent.Future

/** Actor handling users requests and logging history of changes **/
class HistoricalKvActor(implicit inj: Injector)
  extends KvActor
    with akka.actor.ActorLogging {

  import context.dispatcher

  private val historyKvActor = injectActorRef[HistoryKvActor]
  private val kvProcessor = inject[KvProcessor]
  private val storageCache = inject[StorageCache]

  override def receive: Receive = {
    case request: KvGetRequest =>
      process(request, (request: KvGetRequest) => {
        kvProcessor.get(request.storage, request.key).map(r => KvGetResponse(r))
      })
    case KvGetResponse(response) =>
      sender() ! response
    case request: KvMultiGetRequest =>
      process(request, (request: KvMultiGetRequest) => {
        kvProcessor.get(request.storage, request.keys).map(r => KvMultiGetResponse(r))
      })
    case KvMultiGetResponse(response) =>
      sender() ! response
    case request: KvSetRequest =>
      val timestamp = System.currentTimeMillis()
      process(request, (request: KvSetRequest) => {
        kvProcessor.set(request.storage, request.key, request.value).map(r => KvSetResponse(request.storage, request.key, request.value, timestamp, r))
      })
    case KvSetResponse(storage: String, key: String, value: String, timestamp: Long, response: Either[StorageError, Unit]) =>
      logHistory(response, storage, key, value, timestamp, Set)
      sender() ! response
    case request: KvMultiSetRequest =>
      val timestamp = System.currentTimeMillis()
      process(request, (request: KvMultiSetRequest) => {
        kvProcessor.set(request.storage, request.kvs).map(r => KvMultiSetResponse(request.storage, request.kvs, timestamp, r))
      })
    case KvMultiSetResponse(storage: String, kvs: Map[String, String], timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      logHistory(response, storage, timestamp, Set, kvs)
      sender() ! response
    case request: KvDeleteRequest =>
      val timestamp = System.currentTimeMillis()
      process(request, (request: KvDeleteRequest) => {
        kvProcessor.delete(request.storage, request.key).map(r => KvDeleteResponse(request.storage, request.key, timestamp, r))
      })
    case KvDeleteResponse(storage: String, key: String, timestamp: Long, response: Either[StorageError, Unit]) =>
      logHistory(response, storage, key, null, timestamp, Delete)
      sender() ! response
    case request: KvMultiDeleteRequest =>
      val timestamp = System.currentTimeMillis()
      process(request, (request: KvMultiDeleteRequest) => {
        kvProcessor.delete(request.storage, request.keys).map(r => KvMultiDeleteResponse(request.storage, timestamp, r))
      })
    case KvMultiDeleteResponse(storage: String, timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      logHistory(response, storage, timestamp, Delete)
      sender() ! response
    case request: KvListRequest =>
      process(request, (request: KvListRequest) => {
        kvProcessor.list(request.storage).map(r => KvListResponse(r))
      })
    case KvListResponse(response) =>
      sender() ! response
    case request: KvClearRequest =>
      val timestamp = System.currentTimeMillis()
      process(request, (request: KvClearRequest) => {
        kvProcessor.clear(request.storage).map(r => KvClearResponse(request.storage, timestamp, r))
      })
    case KvClearResponse(storage, timestamp, response) =>
      logHistory(response, storage, null, null, timestamp, Clear)
      sender() ! response
    case KvErrorResponse(error) =>
      sender() ! Left(error)
    case failure: Status.Failure =>
      sender() ! Left(InternalError(failure.cause.getMessage))
  }

  protected def process[S <: KvRequest](r: S, producer: S => Future[KvResponse]): Future[KvResponse] = {
    storageCache.get(r.storage).flatMap {
      case Some(_) => producer.apply(r)
      case None => Future(KvErrorResponse(NotFoundError()))
    }.pipeTo(self)(sender())
  }

  protected def logHistory(response: Either[StorageError, Unit], storage: String, key: String, value: String, timestamp: Long, operation: Operation): Unit = {
    response match {
      case Right(_) =>
        storageCache.isHistoryEnabled(storage).map {
          case Some(true) => Some(KvHistory(storage, key, value, timestamp, operation))
          case Some(false) => None
          case None =>
            logError(storage)
            None
        }.pipeTo(historyKvActor)
      case Left(_) => // do nothing
    }
  }

  protected def logHistory(response: Either[StorageError, Map[String, Boolean]], storage: String, timestamp: Long, operation: Operation, values: Map[String, String] = Map()): Unit = {
    response match {
      case Right(results) =>
        storageCache.isHistoryEnabled(storage).map {
          case Some(true) => Some(KvHistoryBulk(results.filter(_._2).map {
            case (key, _) => KvHistory(storage, key, values.getOrElse(key, null), timestamp, operation)
          }))
          case Some(false) => None
          case None =>
            logError(storage)
            None
        }.pipeTo(historyKvActor)
      case Left(_) => // do nothing
    }
  }

  protected def logError(storage: String): Unit = {
    log.error(s"Error while updating storage $storage information.")
  }
}
