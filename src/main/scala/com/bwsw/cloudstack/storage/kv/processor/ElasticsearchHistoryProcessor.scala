package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticsearchHistoryProcessor(client: HttpClient) extends HistoryProcessor {
  protected val `type` = "_doc"

  def save(histories: Vector[KvHistory]): Future[Either[StorageError, Option[List[KvHistory]]]] = {
    val indices = histories.map {
      record =>
        indexInto(getHistoricalStorage(record.storage), `type`) fields getFields(record)
    }
    client.execute(bulk(indices)).map {
      case Left(failure) => Left(getError(failure))
      case Right(success) =>
        val erroneous = success.result.items.filter(_.error.isDefined).map(item => histories(item.itemId))
        if (erroneous.isEmpty)
          Right(None)
        else
          Right(Some(erroneous.toList))
    }
  }

  protected def getHistoricalStorage(storageUuid: String): String = {
    s"history-storage-$storageUuid"
  }

  protected def getError(requestFailure: RequestFailure): InternalError = {
    if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }

  protected def getFields(history: KvHistory): Map[String, Any] = {
    Map(
      "key" -> history.key,
      "value" -> history.value,
      "timestamp" -> history.timestamp,
      "operation" -> history.operation.toString)
  }

}
