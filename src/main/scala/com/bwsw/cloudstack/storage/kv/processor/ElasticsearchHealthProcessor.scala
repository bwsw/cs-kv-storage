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

package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient

import scala.concurrent.Future

class ElasticsearchHealthProcessor(client: HttpClient) extends HealthProcessor {
  private val registryIndex = "storage-registry"

  import scala.concurrent.ExecutionContext.Implicits.global

  def check: Future[Either[StorageError, Boolean]] = {
    val response = for {
      registry <- client.execute(indexExists(registryIndex))
      storageTemplate <- client.execute(getIndexTemplate("storage"))
      historyTemplate <- client.execute(getIndexTemplate("history-storage"))
    } yield (registry, storageTemplate, historyTemplate)
    response.map {
      case (Right(registry), Right(storageTemplate), Right(historyTemplate)) =>
        Right(registry.result.exists && storageTemplate.result.templates.nonEmpty && historyTemplate.result.templates.nonEmpty)
      case _ => Left(InternalError("Elasticsearch error"))
    }

  }
}
