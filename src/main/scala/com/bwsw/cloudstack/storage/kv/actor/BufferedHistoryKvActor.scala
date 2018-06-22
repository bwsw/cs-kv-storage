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

import akka.actor.{Status, Timers}
import akka.pattern.pipe
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.{GetHistoryRequest, ScrollHistoryRequest}
import com.bwsw.cloudstack.storage.kv.message.response.HistoryResponse
import com.bwsw.cloudstack.storage.kv.message.{KvHistory, KvHistoryBulk, KvHistoryFlush, KvHistoryRetry}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/** Actor responsible for history buffering and time or queue size based flushing to storages **/
class BufferedHistoryKvActor(implicit inj: Injector)
  extends HistoryKvActor
    with Timers
    with akka.actor.ActorLogging {

  import context.dispatcher
  import BufferedHistoryKvActor._

  private val buffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val retryBuffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val configuration = inject[AppConfig]
  private val historyProcessor = inject[HistoryProcessor]
  private val storageCache = inject[StorageCache]

  timers.startPeriodicTimer(HistoryTimer, HistoryTimeout, configuration.getFlushHistoryTimeout)

  override def receive: Receive = {
    case HistoryTimeout =>
      self ! flush(buffer)
      self ! flush(retryBuffer)
    case KvHistoryFlush(histories) =>
      historyProcessor.save(histories).map {
        case Some(erroneous) =>
          self ! KvHistoryRetry(erroneous)
        case None => //do nothing
      }
    case KvHistoryRetry(erroneous) =>
      retry(erroneous)
    case option: Option[_] => option match {
      case Some(history: KvHistory) =>
        buffer += history
        if (buffer.length >= configuration.getFlushHistorySize) {
          self ! flush(buffer)
        }
      case Some(bulk: KvHistoryBulk) =>
        buffer.appendAll(bulk.values)
        if (buffer.length >= configuration.getFlushHistorySize) {
          self ! flush(buffer)
        }
      case _ => //do nothing
    }
    case request: GetHistoryRequest =>
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
      }.map(body => HistoryResponse(body)).pipeTo(self)(sender())
    case request: ScrollHistoryRequest =>
      historyProcessor.scroll(request.scrollId, request.timeout).map(body => HistoryResponse(body)).pipeTo(self)(sender())
    case HistoryResponse(body) =>
      sender() ! body
    case failure: Status.Failure =>
      sender() ! Left(InternalError(failure.cause.getMessage))
  }

  private def retry(histories: Iterable[KvHistory]): Unit = {
    retryBuffer.appendAll(histories.filter(filterWithErrorLogging).map(history => history.makeAttempt))
    if (retryBuffer.length >= configuration.getFlushHistorySize) {
      self ! flush(retryBuffer)
    }
  }

  private def filterWithErrorLogging = {
    history: KvHistory =>
      if (history.attempt < configuration.getHistoryRetryLimit) {
        true
      }
      else {
        log.error("Failed to save " + history.toString)
        false
      }
  }

  private def flush(aBuffer: ListBuffer[KvHistory]): KvHistoryFlush = {
    val values = aBuffer.toList
    aBuffer.clear()
    KvHistoryFlush(values)
  }
}

object BufferedHistoryKvActor {

  private case object HistoryTimer

  private case object HistoryTimeout

}
