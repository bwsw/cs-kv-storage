package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.entity.Storage

import scala.concurrent.Future

trait StorageCache {
  def isHistoryEnabled(storageUuid: String): Future[Option[Boolean]]

  def get(storageUuid: String): Future[Option[Storage]]
}
