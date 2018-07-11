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

package com.bwsw.cloudstack.storage.kv.manager

import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils.{DocumentType, RegistryIndex, TemporaryStorageType,
  getError}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestSuccess}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/** A manager for Elasticsearch storages.
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchKvStorageManager(client: HttpClient, cache: StorageCache) extends KvStorageManager {

  import ElasticsearchKvStorageManager._

  import scala.concurrent.ExecutionContext.Implicits.global

  def updateTempStorageTtl(storage: String, ttl: Long): Future[Either[StorageError, Unit]] = {
    process(
      storage,
      s"if (ctx._source.type == '$TemporaryStorageType') { ctx._source.expiration_timestamp = ctx._source" +
        s".expiration_timestamp - ctx._source.ttl + $ttl; ctx._source.ttl = $ttl } else { ctx.op='noop' }",
      delete = false)
  }

  def deleteTempStorage(storage: String): Future[Either[StorageError, Unit]] = {
    process(
      storage,
      s"if (ctx._source.type == '$TemporaryStorageType') { ctx._source.deleted = true } else { ctx.op='noop' }",
      delete = true)
  }

  private def process(storage: String, scriptCode: String, delete: Boolean) = {
    client.execute(update(storage) in RegistryIndex / DocumentType script scriptCode).map {
      case Left(failure) => failure.status match {
        case 404 => Left(NotFoundError())
        case _ =>
          logger.error("Elasticsearch update request failure: {}", failure.error)
          Left(getError(failure))
      }
      case Right(RequestSuccess(_, _, _, updateRequest)) => updateRequest.result match {
        case "updated" =>
          if (delete)
            cache.delete(storage)
          Right(())
        case "noop" => Left(BadRequestError())
      }
    }
  }
}

/** ElasticsearchKvStorageManager companion object. **/
object ElasticsearchKvStorageManager {
  private val logger = LoggerFactory.getLogger(getClass)
}
