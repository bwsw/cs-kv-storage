package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.entity.Storage

import scala.concurrent.Future

trait StorageLoader {
  def load: String => Future[Option[Storage]]
}
