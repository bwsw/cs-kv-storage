package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.app.Configuration
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}
import com.sksamuel.elastic4s.http.HttpClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class HistoryProcessor(conf: Configuration, client: HttpClient) {
  private val cacheStorage: AsyncLoadingCache[String, String] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getStorageCacheExpirationTime))
      .maximumSize(conf.getMaxStorageCacheSize)
      .buildAsyncFuture((i: String) => client.execute())
}
