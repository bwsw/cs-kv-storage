package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Status
import akka.pattern.pipe
import com.bwsw.cloudstack.storage.kv.entity.StorageCache
import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.message.response._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import scaldi.Injector
import scaldi.akka.AkkaInjectable._

class HistoricalKvActor(implicit inj: Injector) extends KvActor {

  import context.dispatcher

  private val historyKvActor = injectActorRef[HistoryKvActor]
  private val kvProcessor = inject[KvProcessor]
  private val storageCache = inject[StorageCache]

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
      tryUpdateHistory(response, storage, key, value, timestamp, "set")
      sender() ! response
    case KvMultiSetRequest(storage: String, kvs: Map[String, String]) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.set(storage, kvs).map(r => KvMultiSetResponse(storage, kvs, timestamp, r)).pipeTo(self)(sender())
    case KvMultiSetResponse(storage: String, kvs: Map[String, String], timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      tryUpdateHistory(response, storage, timestamp, "set", kvs)
      sender() ! response
    case KvDeleteRequest(storage: String, key: String) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.delete(storage, key).map(r => KvDeleteResponse(storage, key, timestamp, r)).pipeTo(self)(sender())
    case KvDeleteResponse(storage: String, key: String, timestamp: Long, response: Either[StorageError, Unit]) =>
      tryUpdateHistory(response, storage, key, null, timestamp, "delete")
      sender() ! response
    case KvMultiDeleteRequest(storage: String, keys: Iterable[String]) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.delete(storage, keys).map(r => KvMultiDeleteResponse(storage, timestamp, r)).pipeTo(self)(sender())
    case KvMultiDeleteResponse(storage: String, timestamp: Long, response: Either[StorageError, Map[String, Boolean]]) =>
      tryUpdateHistory(response, storage, timestamp, "delete")
      sender() ! response
    case KvListRequest(storage: String) =>
      kvProcessor.list(storage).map(r => KvListResponse(r)).pipeTo(self)(sender())
    case KvListResponse(response) =>
      sender() ! response
    case KvClearRequest(storage: String) =>
      val timestamp = System.currentTimeMillis()
      kvProcessor.clear(storage).map(r => KvClearResponse(storage, timestamp, r)).pipeTo(self)(sender())
    case KvClearResponse(storage, timestamp, response) =>
      tryUpdateHistory(response, storage, null, null, timestamp, "clear")
      sender() ! response
    case failure: Status.Failure =>
      sender() ! Left(InternalError(failure.cause.getMessage))
  }

  protected def tryUpdateHistory[A, B](response: Either[A, B], storage: String, key: String, value: String, timestamp: Long, operation: String): Unit = {
    response match {
      case Right(_) =>
        storageCache.isHistoryEnabled(storage).map {
          case Some(true) => Some(KvHistory(storage, key, value, timestamp, operation))
          case Some(false) => None
          case None => None //TODO: process error
        }.pipeTo(historyKvActor)
      case Left(_) => // do nothing
    }
  }

  protected def tryUpdateHistory[A](response: Either[A, Map[String, Boolean]], storage: String, timestamp: Long, operation: String, values: Map[String, String] = Map()): Unit = {
    response match {
      case Right(results) =>
        storageCache.isHistoryEnabled(storage).map {
          case Some(true) => Some(KvHistoryBulk(results.filter(_._2).map {
            case (key, result) => KvHistory(storage, key, values.getOrElse(key, null), timestamp, operation)
          }))
          case Some(false) => None
          case None => None //TODO: process error
        }.pipeTo(historyKvActor)
      case Left(_) => // do nothing
    }
  }
}
