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

  import BufferedHistoryKvActor._
  import context.dispatcher

  private val buffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val retryBuffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val configuration = inject[AppConfig]
  private val historyProcessor = inject[HistoryProcessor]

  timers.startPeriodicTimer(HistoryTimer, HistoryTimeout, configuration.getFlushHistoryTimeout)

  override def postStop() {
    if (buffer.size + retryBuffer.size > 0){
      log.info("BufferedHistoryKvActor shutdown initiated. Flushing {} records.", buffer.size + retryBuffer.size)
      val flushSize = configuration.getFlushHistorySize
      val resultList = (buffer ++ retryBuffer).grouped(flushSize).toList.map(batch => historyProcessor.save(batch.toList))
      Future.foldLeft(resultList)(0) {
        case (sum, None) => sum
        case (sum, Some(list)) => sum + list.size
      }.map { erroneousNum =>
        if (erroneousNum > 0)
          log.error("BufferedHistoryKvActor unable to save {} histories on shutdown.", erroneousNum)
        else
          log.info("BufferedHistoryKvActor shut down successfully.")
      }
    }
  }

  override def receive: Receive = {
    case HistoryTimeout =>
      if (buffer.nonEmpty || retryBuffer.nonEmpty) {
        log.info("BufferedHistoryKvActor: flush of {} records by timeout.", buffer.size + retryBuffer.size)
        flush(buffer)
        flush(retryBuffer)
      }
    case KvHistoryFlush(histories) =>
      historyProcessor.save(histories).map {
        case Some(erroneous) =>
          self ! KvHistoryRetry(erroneous)
        case None => //do nothing
      }.recover{ case failure =>
        log.error(failure.getCause, "Failure during flush processing")
        self ! KvHistoryRetry(histories)
      }
    case KvHistoryRetry(erroneous) =>
      retry(erroneous)
    case history: KvHistory =>
      buffer += history
      if (buffer.length >= configuration.getFlushHistorySize) {
        flush(buffer)
      }
    case bulk: KvHistoryBulk =>
      buffer.appendAll(bulk.values)
      if (buffer.length >= configuration.getFlushHistorySize) {
        flush(buffer)
      }
  }

  private def retry(histories: Iterable[KvHistory]): Unit = {
    retryBuffer.appendAll(histories.filter(filterWithErrorLogging).map(history => history.makeAttempt))
  }

  private def filterWithErrorLogging = {
    history: KvHistory =>
      if (history.attempt < configuration.getHistoryRetryLimit) {
        true
      }
      else {
        log.error("Failed to save {}", history.toString)
        false
      }
  }

  private def flush(flushBuffer: ListBuffer[KvHistory]) = {
    val flushSize = configuration.getFlushHistorySize
    flushBuffer.grouped(flushSize).foreach(batch => self ! KvHistoryFlush(batch.result()))
    flushBuffer.clear()
  }
}

object BufferedHistoryKvActor {

  private case object HistoryTimer

  private case object HistoryTimeout

}
