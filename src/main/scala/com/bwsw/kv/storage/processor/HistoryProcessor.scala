package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.app.Configuration
import com.bwsw.kv.storage.models.storage.Storage
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import java.util.concurrent._

import com.bwsw.kv.storage.models.HistoryEntry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class HistoryProcessor(conf: Configuration, client: HttpClient) {
  private val registry = "storage-registry"
  private val `type` = "_doc"
  val cacheStorage: AsyncLoadingCache[String, Option[Storage]] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getStorageCacheExpirationTime))
      .maximumSize(conf.getMaxStorageCacheSize)
      .buildAsyncFuture((id: String) =>
        client.execute(get(registry, `type`, id)).map {
          case Left(_) => throw new RuntimeException("Storage info loading failed")
          case Right(success) =>
            if (success.result.found) {
              Some(Storage(success.result.id, getValue(success.result.source, "type"), getValue(success.result.source, "is_history_enabled").toBoolean))
            }
            else None
        })
  val queue: ConcurrentLinkedQueue[HistoryEntry] = new ConcurrentLinkedQueue[HistoryEntry]()

  def process(storageUuid: String, key: String, value: String, timestamp: Long): Future[Unit] = {
    cacheStorage.get(storageUuid).map {
      case None => Unit
      case Some(storage) =>
        if (storage.isHistoryEnabled)
          queue.add(HistoryEntry(storageUuid, key, value, timestamp))
        else
          Unit
    }
  }

  private def getValue(source: Map[String, Any], key: String) = {
    source.get(key) match {
      case Some(s: String) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }
}
