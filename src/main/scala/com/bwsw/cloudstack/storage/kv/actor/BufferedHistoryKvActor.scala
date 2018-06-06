package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.historian.KvHistorian
import com.bwsw.cloudstack.storage.kv.message.{KvHistory, KvHistoryBulk, KvHistoryFlush}
import scaldi.Injector
import scaldi.akka.AkkaInjectable._


import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer

class BufferedHistoryKvActor(implicit inj: Injector) extends HistoryKvActor with Timers {

  import BufferedHistoryKvActor._

  private val buffer: ListBuffer[(KvHistory, Int)] = ListBuffer.empty
  private val configuration = inject[AppConfig]
  private val kvHistorian = inject[KvHistorian]

  timers.startPeriodicTimer(HistoryTimer, HistoryTimeout, configuration.getFlushHistoryTimeout)

  override def receive: Receive = {
    case HistoryTimeout ⇒
      self ! flush()
    case KvHistoryFlush(historiesWithRepeats) =>
      val histories = historiesWithRepeats.keys.toVector
      kvHistorian.save(histories).map {
        case Right(results) =>
          buffer.appendAll(historiesWithRepeats.filter { case (history, count) =>
            !results(history) && (count < configuration.getHistoryRetryLimit)
          }.map { case (history, count) => (history, count + 1) })
        case Left(_) => //do nothing

      }
    case option: Option[_] => option match {
      case Some(history: KvHistory) =>
        buffer += Tuple2(history, 1)
        if (buffer.length == configuration.getFlushHistorySize) {
          self ! flush()
        }
      case Some(bulk: KvHistoryBulk) => bulk.values.foreach(history => self ! Some(history))
      case _ => //do nothing
    }
  }

  private def flush(): KvHistoryFlush = {
    val values = buffer.toMap
    buffer.clear()
    KvHistoryFlush(values)
  }
}

object BufferedHistoryKvActor {

  private case object HistoryTimer

  private case object HistoryTimeout

}
