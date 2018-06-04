package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.app.Configuration
import com.bwsw.cloudstack.storage.kv.message.{KvHistory, KvHistoryFlush}
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

import scala.collection.mutable.ListBuffer

class BufferedHistoryKvActor(implicit inj: Injector) extends HistoryKvActor with Timers {

  import BufferedHistoryKvActor._

  private val buffer: ListBuffer[KvHistory] = ListBuffer.empty
  private val configuration = inject[Configuration]

  timers.startPeriodicTimer(HistoryTimer, HistoryTimeout, configuration.getFlushHistoryTimeout)

  override def receive: Receive = {
    case HistoryTimeout ⇒
      self ! flush()
    case KvHistoryFlush(values) =>
    // TODO: write history
    case history: KvHistory =>
      buffer += history
      if (buffer.length == configuration.getFlushHistorySize) {
        self ! flush()
      }
  }

  private def flush(): KvHistoryFlush = {
    val values = buffer.toList
    buffer.clear()
    KvHistoryFlush(values)
  }
}

object BufferedHistoryKvActor {

  private case object HistoryTimer

  private case object HistoryTimeout

}
