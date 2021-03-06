// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.cache

import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.github.blemale.scaffeine.{AsyncLoadingCache, Scaffeine}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** StorageCache that loads data on demand using loader **/
class LoadingStorageCache(conf: AppConfig, loader: StorageLoader) extends StorageCache {
  val cache: AsyncLoadingCache[String, Option[Storage]] =
    Scaffeine()
      .recordStats()
      .expireAfterWrite(Duration(conf.getCacheExpirationTime))
      .maximumSize(conf.getMaxCacheSize)
      .buildAsyncFuture(loader.load)

  def get(storageUuid: String): Future[Option[Storage]] = {
    cache.get(storageUuid)
  }

  def invalidateAll(): Unit = {
    cache.synchronous.invalidateAll()
  }

  def invalidateAll(keys: Iterable[String]): Unit = {
    cache.synchronous.invalidateAll(keys)
  }

  def delete(storageUuid: String): Unit = {
    cache.put(storageUuid, Future(None))
  }
}
