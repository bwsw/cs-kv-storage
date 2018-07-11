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
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils._
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}

import scala.concurrent.Future

/** A manager for Elasticsearch storages.
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchKvStorageManager(client: HttpClient, cache: StorageCache) extends KvStorageManager {

  import ElasticsearchKvStorageManager._

  import scala.concurrent.ExecutionContext.Implicits.global

  def updateTempStorageTtl(storage: String, ttl: Long): Future[Either[StorageError, Unit]] = {
    client.execute(update(storage) in RegistryIndex / DocumentType script
                     s"if (ctx._source.type == '$TemporaryStorageType')" +
                       s"{ ctx._source.expiration_timestamp = ctx._source.expiration_timestamp - ctx._source.ttl + " +
                       s"$ttl; ctx._source.ttl = $ttl } else { ctx.op='noop'}")
      .map {
        case Left(failure) => failure.status match {
          case 404 => Left(NotFoundError())
          case _ => Left(getError(failure))
        }
        case Right(RequestSuccess(status, body, headers, updateRequest)) => updateRequest.result match {
          case "updated" => Right(Unit)
          case "noop" => Left(BadRequestError())
        }
      }
  }

  def deleteTempStorage(storage: String): Future[Either[StorageError, Unit]] = {
    client.execute(update(storage) in RegistryIndex / DocumentType script
                     s"""if (ctx._source.type == '$TemporaryStorageType')""" +
                       s"""{ ctx._source.deleted = true } else { ctx.op='noop'}""")
      .map {
        case Left(failure) => failure.status match {
          case 404 => Left(NotFoundError())
          case _ => Left(getError(failure))
        }
        case Right(RequestSuccess(status, body, headers, updateRequest)) => updateRequest.result match {
          case "updated" =>
            cache.delete(storage)
            Right(Unit)
          case "noop" => Left(BadRequestError())
        }
      }
  }
}

/** ElasticsearchKvStorageManager companion object. **/
object ElasticsearchKvStorageManager {
  private def getError(requestFailure: RequestFailure): StorageError = {
    if (requestFailure.status == 404)
      NotFoundError()
    else if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }
}
