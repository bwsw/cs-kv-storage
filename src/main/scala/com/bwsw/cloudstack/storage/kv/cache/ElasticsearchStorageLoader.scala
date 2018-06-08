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
import com.sksamuel.elastic4s.http.ElasticDsl.get
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** Provides access to information about storages existing in Elasticsearch **/
class ElasticsearchStorageLoader(client: HttpClient) extends StorageLoader {
  private val registry = "storage-registry"
  private val `type` = "_doc"

  def load: String => Future[Option[Storage]] = {
    id: String =>
      client.execute(get(registry, `type`, id)).map {
        case Left(_) => throw new RuntimeException("Storage info loading failed")
        case Right(success) =>
          if (success.result.found) {
            Some(Storage(success.result.id, getValue(success.result.source, "type"), getValue(success.result.source, "is_history_enabled").toBoolean))
          }
          else None
      }
  }

  private def getValue(source: Map[String, Any], key: String) = {
    source.get(key) match {
      case Some(s: String) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }
}