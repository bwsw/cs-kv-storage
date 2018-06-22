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

import com.bwsw.cloudstack.storage.kv.actor.{BufferedHistoryKvActor, HistoricalKvActor, HistoryKvActor, KvActor}
import com.bwsw.cloudstack.storage.kv.cache.{ElasticsearchStorageLoader, LoadingStorageCache, StorageCache, StorageLoader}
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.manager.{ElasticsearchKvStorageManager, KvStorageManager}
import com.bwsw.cloudstack.storage.kv.processor.{ElasticsearchHistoryProcessor, ElasticsearchKvProcessor, HistoryProcessor, KvProcessor}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import scaldi.Module

class KvStorageModule extends Module {

  val appConfig = new AppConfig
  val elasticsearchConfig = new ElasticsearchConfig

  bind[AppConfig] toNonLazy appConfig
  bind[ElasticsearchConfig] toNonLazy elasticsearchConfig

  bind[HttpClient] toNonLazy HttpClient(ElasticsearchClientUri(elasticsearchConfig.getUri))
  bind[KvProcessor] to injected[ElasticsearchKvProcessor]
  bind[KvStorageManager] to injected[ElasticsearchKvStorageManager]
  bind[HistoryKvActor] to injected[BufferedHistoryKvActor]
  bind[KvActor] to injected[HistoricalKvActor]
  bind[HistoryProcessor] to injected[ElasticsearchHistoryProcessor]
  bind[StorageLoader] to injected[ElasticsearchStorageLoader]
  bind[StorageCache] to injected[LoadingStorageCache]
}
