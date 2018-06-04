package com.bwsw.cloudstack.storage.kv.entity

import com.bwsw.cloudstack.storage.kv.app.Configuration
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import scala.concurrent, scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.Duration

class StorageCache(conf: Configuration, client: HttpClient) {
  private val registry = "storage-registry"
  private val `type` = "_doc"
  val cache: AsyncLoadingCache[String, Option[(String, String, Boolean)]] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getStorageCacheExpirationTime))
      .maximumSize(conf.getMaxStorageCacheSize)
      .buildAsyncFuture((id: String) =>
        client.execute(get(registry, `type`, id)).map {
          case Left(_) => throw new RuntimeException("Storage info loading failed")
          case Right(success) =>
            if (success.result.found) {
              Some((success.result.id, getValue(success.result.source, "type"), getValue(success.result.source, "is_history_enabled").toBoolean))
            }
            else None
        })

  private def getValue(source: Map[String, Any], key: String) = {
    source.get(key) match {
      case Some(s: String) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }
}
