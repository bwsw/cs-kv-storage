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
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError,
  UnauthorizedError}
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.RegistryFields._
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.ScriptOperations._
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.StorageType.Temporary
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{DocumentType, RegistryIndex, StorageType, getError}
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

  def updateTempStorageTtl(
      storageUuid: String,
      secretKey: Array[Char],
      ttl: Long): Future[Either[StorageError, Unit]] = {
    process(
      storageUuid,
      secretKey,
      s"if (ctx._source.$Type == '$Temporary') { ctx._source.$ExpirationTimestamp = ctx._source" +
        s".$ExpirationTimestamp - ctx._source.$Ttl + $ttl; ctx._source.$Ttl = $ttl } else { ctx.op='$NoOp' }",
      delete = false)
  }

  def deleteTempStorage(storageUuid: String, secretKey: Array[Char]): Future[Either[StorageError, Unit]] = {
    process(
      storageUuid,
      secretKey,
      s"if (ctx._source.$Type == '$Temporary') { ctx._source.$Deleted = true } else { ctx.op='$NoOp' }",
      delete = true)
  }

  private def process(storageUuid: String, secretKey: Array[Char], scriptCode: String, delete: Boolean) = {
    cache.get(storageUuid).flatMap {
      case Some(storage) =>
        if (secretKey.sameElements(storage.secretKey))
          if (storage.storageType == StorageType.Temporary)
            client.execute(update(storageUuid) in RegistryIndex / DocumentType script scriptCode).map
            {//TODO: refactor without using script
              case Left(failure) => failure.status match {
                case 404 =>
                  cache.delete(storageUuid)
                  Left(NotFoundError())
                case _ =>
                  logger.error("Elasticsearch update request failure: {}", failure.error)
                  Left(getError(failure))
              }
              case Right(RequestSuccess(_, _, _, updateRequest)) => updateRequest.result match {
                case Updated =>
                  if (delete)
                    cache.delete(storageUuid)
                  Right(())
                case NoOp => Left(BadRequestError())
              }
            }
          else
            Future(Left(BadRequestError()))
        else
          Future(Left(UnauthorizedError()))
      case None =>
        Future(Left(NotFoundError()))
    }.recover { case ex =>
      logger.error(ex.getMessage, ex.getCause)
      Left(InternalError(ex.getMessage))
    }
  }
}

/** ElasticsearchKvStorageManager companion object. **/
object ElasticsearchKvStorageManager {
  private val logger = LoggerFactory.getLogger(getClass)
}
