package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class LoadingStorageCache(conf: AppConfig, loader: StorageLoader) extends StorageCache {
  val cache: AsyncLoadingCache[String, Option[Storage]] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getCacheExpirationTime))
      .maximumSize(conf.getMaxCacheSize)
      .buildAsyncFuture(loader.load)

  def isHistoryEnabled(storageUuid: String): Future[Option[Boolean]] = {
    cache.get(storageUuid).map {
      case Some(storage) => Some(storage.keepHistory && storage.storageType != "TEMP")
      case None => None
    }
  }

  def get(storageUuid: String): Future[Option[Storage]] = {
    cache.get(storageUuid)
  }

}
