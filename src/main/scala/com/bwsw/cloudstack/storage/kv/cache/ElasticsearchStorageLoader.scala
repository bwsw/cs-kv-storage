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

import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{RegistryIndex, DocumentType, RegistryFields, DefaultError}
import com.sksamuel.elastic4s.http.ElasticDsl.{get, _}
import com.sksamuel.elastic4s.http.HttpClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** Provides access to information about storages existing in Elasticsearch **/
class ElasticsearchStorageLoader(client: HttpClient) extends StorageLoader {

  def load: String => Future[Option[Storage]] = {
    id: String =>
      client.execute(get(RegistryIndex, DocumentType, id)).map {
        case Left(_) => throw new RuntimeException("Storage info loading failed")
        case Right(success) =>
          if (success.result.found && !getValue[Boolean](success.result.source, RegistryFields.Deleted)) {
            Some(Storage(
              success.result.id,
              getValue(success.result.source, RegistryFields.Type),
              getValue(success.result.source, RegistryFields.HistoryEnabled)))
          }
          else None
      }
  }

  private def getValue[T](source: Map[String, Any], key: String): T = source.get(key) match {
    case Some(s) => s.asInstanceOf[T]
    case None => throw new RuntimeException(DefaultError)
  }
}
