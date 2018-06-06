package com.bwsw.cloudstack.storage.kv.app

import com.bwsw.cloudstack.storage.kv.actor.{BufferedHistoryKvActor, HistoricalKvActor, HistoryKvActor, KvActor}
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.entity.StorageCache
import com.bwsw.cloudstack.storage.kv.historian.{ElasticsearchKvHistorian, KvHistorian}
import com.bwsw.cloudstack.storage.kv.manager.{ElasticsearchKvStorageManager, KvStorageManager}
import com.bwsw.cloudstack.storage.kv.processor.{ElasticsearchKvProcessor, KvProcessor}
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
  bind[KvHistorian] to injected[ElasticsearchKvHistorian]
  bind[StorageCache] to injected[StorageCache]
}
