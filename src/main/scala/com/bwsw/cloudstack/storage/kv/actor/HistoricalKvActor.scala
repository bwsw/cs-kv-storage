package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Status
import akka.pattern.pipe
import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

class HistoricalKvActor(implicit inj: Injector) extends KvActor {

  import context.dispatcher

  private val historyKvActor = injectActorRef[HistoryKvActor]
  private val kvProcessor = inject[KvProcessor]

  override def receive: Receive = {
    case KvGetRequest(storage: String, key: String) =>
      kvProcessor.get(storage, key).map(r => KvGetResponse(r)).pipeTo(self)(sender())
    case KvGetResponse(response) =>
      sender() ! response
    case KvSetRequest(storage: String, key: String, value: String) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.set(storage, key, value).map(r => KvSetResponse(storage, key, value, timestamp, r)).pipeTo(self)(sender())
    case KvSetResponse(storage: String, key: String, value: String, timestamp: Long, response: Either[StorageError, Unit]) =>
      response match {
        case Right(_) =>
          historyKvActor ! KvHistory(storage, key, value, timestamp)
        case Left(_) => // do nothing
      }
      sender() ! response
    case failure: Status.Failure =>
      sender() ! Left(InternalError(failure.cause.getMessage))
  }
}
