package com.bwsw.cloudstack.storage.kv.app

import com.bwsw.cloudstack.storage.kv.actor._
import com.bwsw.cloudstack.storage.kv.cache.{ElasticsearchStorageLoader, LoadingStorageCache, StorageCache, StorageLoader}
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.manager.{ElasticsearchKvStorageManager, KvStorageManager}
import com.bwsw.cloudstack.storage.kv.processor.{ElasticsearchHistoryProcessor, ElasticsearchKvProcessor, HistoryProcessor, KvProcessor}
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import scaldi.Module

class KvStorageModule extends Module {

  val appConfig = new AppConfig
  val elasticsearchConfig = new ElasticsearchConfig
  val clock = new Clock

  bind[Clock] toNonLazy clock
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
  bind[HealthActor] to injected[ElasticsearchHealthActor]
  bind[TemplateCheckActor] to injected[ElasticsearchTemplateCheckActor]
}
