package com.bwsw.cloudstack.storage.kv.historian

import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}
import com.sksamuel.elastic4s.http.ElasticDsl._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticsearchKvHistorian(client: HttpClient) extends KvHistorian {
  protected val `type` = "_doc"

  /** Saves historical record into dedicated storage
    */
  def save(history: KvHistory): Future[Either[StorageError, Unit]] = {
    client.execute(indexInto(getHistoricalStorage(history.storage), `type`) fields Map(
      "key" -> history.key,
      "value" -> history.value,
      "timestamp" -> history.timestamp))
      .map {
        case Left(failure) => Left(getError(failure))
        case Right(_) => Right(Unit)
      }
  }

  /** Saves collection af historical records into dedicated storage
    */
  def save(histories: Iterable[KvHistory]): Future[Either[StorageError, Map[String, Int]]] = {
    val indices = histories.map {
      record =>
        indexInto(getHistoricalStorage(record.storage), `type`) fields Map(
          "key" -> record.key,
          "value" -> record.value,
          "timestamp" -> record.timestamp
        )
    }
    client.execute(bulk(indices)).map {
      case Left(failure) => Left(getError(failure))
      case Right(success) =>
        Right(success.result.items.groupBy(bulkResponseItem => bulkResponseItem.index).map {
          case (index, items) => (index, items.count(item => item.error.isEmpty))
        })
      //        Right(success.result.items.map(bulkResponseItem =>
      //          ((bulkResponseItem.index, bulkResponseItem.id), bulkResponseItem.error.isEmpty)).toMap)
    }
  }

  protected def getHistoricalStorage(storageUuid: String): String = {
    s"storage-$storageUuid-history"
  }

  protected def getError(requestFailure: RequestFailure): InternalError = {
    if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }

}
