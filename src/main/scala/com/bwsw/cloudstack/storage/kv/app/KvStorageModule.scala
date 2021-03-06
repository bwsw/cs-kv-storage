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

package com.bwsw.cloudstack.storage.kv.app

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.bwsw.cloudstack.storage.kv.actor._
import com.bwsw.cloudstack.storage.kv.cache.{ElasticsearchStorageLoader, LoadingStorageCache, StorageCache,
  StorageLoader}
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.manager.{ElasticsearchKvStorageManager, KvStorageManager}
import com.bwsw.cloudstack.storage.kv.processor.{ElasticsearchHistoryProcessor, ElasticsearchKvProcessor,
  HistoryProcessor, KvProcessor}
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import scaldi.Module
import scaldi.akka.AkkaInjectable

class KvStorageModule extends Module {

  val appConfig = new AppConfig
  val elasticsearchConfig = new ElasticsearchConfig
  val clock = new Clock

  implicit val actorSystem: ActorSystem = ActorSystem("cs-kv-storage")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  val client: HttpClient = if (elasticsearchConfig.isAuthEnabled) {
    lazy val provider: BasicCredentialsProvider = {
      val provider = new BasicCredentialsProvider
      val credentials = new UsernamePasswordCredentials(
        elasticsearchConfig.getUsername,
        elasticsearchConfig.getPassword)
      provider.setCredentials(AuthScope.ANY, credentials)
      provider
    }
    HttpClient(
      ElasticsearchClientUri(elasticsearchConfig.getUri),
      (requestConfigBuilder: RequestConfig.Builder) => {
        requestConfigBuilder
      },
      (httpClientBuilder: HttpAsyncClientBuilder) => {
        httpClientBuilder.setDefaultCredentialsProvider(provider)
      })
  } else {
    HttpClient(ElasticsearchClientUri(elasticsearchConfig.getUri))
  }

  bind[Clock] toNonLazy clock
  bind[AppConfig] toNonLazy appConfig
  bind[ElasticsearchConfig] toNonLazy elasticsearchConfig

  bind[ActorSystem] toNonLazy actorSystem
  bind[Materializer] toNonLazy actorMaterializer

  bind[HttpClient] toNonLazy client
  bind[KvProcessor] to injected[ElasticsearchKvProcessor]
  bind[KvStorageManager] to injected[ElasticsearchKvStorageManager]
  bind[HistoryKvActor] to injected[BufferedHistoryKvActor]
  bind[KvActor] to injected[HistoricalKvActor]
  bind[HistoryRequestActor] to injected[DefaultHistoryRequestActor]
  bind[HistoryProcessor] to injected[ElasticsearchHistoryProcessor]
  bind[StorageLoader] to injected[ElasticsearchStorageLoader]
  bind[StorageCache] to injected[LoadingStorageCache]
  bind[HealthActor] to injected[ElasticsearchHealthActor]
  bind[TemplateCheckActor] to injected[ElasticsearchTemplateCheckActor]

  bind[CacheUpdateActor] toProvider new ElasticsearchCacheUpdateActor
  binding identifiedBy 'cacheUpdateActor to {
    AkkaInjectable.injectActorRef[CacheUpdateActor]
  }
}
