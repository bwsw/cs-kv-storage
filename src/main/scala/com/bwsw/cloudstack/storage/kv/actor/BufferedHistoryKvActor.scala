package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.message.{KvHistory, KvHistoryBulk, KvHistoryFlush}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

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
        case Right(Some(erroneous)) =>
          retry(erroneous)
        case Right(None) => //do nothing
        case Left(_) =>
          retry(histories)
      }
    case option: Option[_] => option match {
      case Some(history: KvHistory) =>
        buffer += history
        if (buffer.length == configuration.getFlushHistorySize) {
          self ! flush(buffer)
        }
      case Some(bulk: KvHistoryBulk) => bulk.values.foreach(history => self ! Some(history))
      case _ => //do nothing
    }
  }

  private def retry(histories: Iterable[KvHistory]): Unit = {
    retryBuffer.appendAll(histories.filter(filterWithErrorLogging).map(history => history.makeAttempt))
    if (retryBuffer.length == configuration.getFlushHistorySize) {
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
    val values = aBuffer.toVector
    aBuffer.clear()
    KvHistoryFlush(values)
  }
}

object BufferedHistoryKvActor {

  private case object HistoryTimer

  private case object HistoryTimeout

}
