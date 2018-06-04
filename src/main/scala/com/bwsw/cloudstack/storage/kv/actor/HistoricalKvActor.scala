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
    case KvMultiGetRequest(storage: String, keys: Iterable[String]) =>
      kvProcessor.get(storage, keys).map(r => KvMultiGetResponse(r)).pipeTo(self)(sender())
    case KvMultiGetResponse(response) =>
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
    case KvMultiSetRequest(storage: String, kvs: Map[String, String]) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.set(storage, kvs).map(r => KvMultiSetResponse(storage, kvs, timestamp, r)).pipeTo(self)(sender())
    case KvMultiSetResponse(storage: String, kvs: Map[String, String], timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      response match {
        case Right(results) =>
          results.foreach { case (key, isSet) =>
            if (isSet)
              historyKvActor ! KvHistory(storage, key, kvs(key), timestamp)
          }
        case Left(_) => // do nothing
      }
      sender() ! response
    case KvDeleteRequest(storage: String, key: String) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.delete(storage, key).map(r => KvDeleteResponse(storage, key, timestamp, r)).pipeTo(self)(sender())
    case KvDeleteResponse(storage: String, key: String, timestamp: Long, response: Either[StorageError, Unit]) =>
      response match {
        case Right(_) =>
          historyKvActor ! KvHistory(storage, key, null, timestamp)
        case Left(_) => // do nothing
      }
      sender() ! response
    case KvMultiDeleteRequest(storage: String, keys: Iterable[String]) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.delete(storage, keys).map(r => KvMultiDeleteResponse(storage, keys, timestamp, r)).pipeTo(self)(sender())
    case KvMultiDeleteResponse(storage: String, keys: Iterable[String], timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      response match {
        case Right(results) =>
          results.foreach { case (key, isDeleted) =>
            if (isDeleted)
              historyKvActor ! KvHistory(storage, key, null, timestamp)
          }
        case Left(_) => // do nothing
      }
      sender() ! response
    case KvListRequest(storage: String) =>
      kvProcessor.list(storage).map(r => KvListResponse(r)).pipeTo(self)(sender())
    case KvListResponse(response) =>
      sender() ! response
    case KvClearRequest(storage: String) =>
      kvProcessor.clear(storage).map(r => KvClearResponse(r)).pipeTo(self)(sender())
    case KvClearResponse(response) =>
      //TODO: Maintain history
      sender() ! response
    case failure: Status.Failure =>
      sender() ! Left(InternalError(failure.cause.getMessage))
  }
}
