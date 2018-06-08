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

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.message.{HistoryRetry, KvHistory, KvHistoryBulk, KvHistoryFlush}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

/** Actor responsible for history buffering and time or queue size based flushing to storages **/
class BufferedHistoryKvActor(implicit inj: Injector)
  extends HistoryKvActor
    with Timers
    with akka.actor.ActorLogging {

  import BufferedHistoryKvActor._

  private val buffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val retryBuffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val configuration = inject[AppConfig]
  private val historyProcessor = inject[HistoryProcessor]

  timers.startPeriodicTimer(HistoryTimer, HistoryTimeout, configuration.getFlushHistoryTimeout)

  override def receive: Receive = {
    case HistoryTimeout =>
      self ! flush(buffer)
      self ! flush(retryBuffer)
    case KvHistoryFlush(histories) =>
      historyProcessor.save(histories).map {
        case Some(erroneous) =>
          self ! HistoryRetry(erroneous)
        case None => //do nothing
      }
    case HistoryRetry(erroneous) =>
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