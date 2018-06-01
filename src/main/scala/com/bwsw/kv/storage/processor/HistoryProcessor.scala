package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.app.Configuration
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class HistoryProcessor(conf: Configuration, client: HttpClient) {
  private val registry = "storage-registry"
  private val `type` = "_doc"

  private val cacheStorage: AsyncLoadingCache[String, Map[String, Any]] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getStorageCacheExpirationTime))
      .maximumSize(conf.getMaxStorageCacheSize)
      .buildAsyncFuture((id: String) =>
        client.execute(get(registry, `type`, id)).map {
          case Left(failure) => throw new RuntimeException("Storage info loading failed")
          case Right(success) =>
            if (success.result.found)
              success.result.source
            else
              null
        })
}
