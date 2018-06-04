package com.bwsw.cloudstack.storage.kv.app

import com.bwsw.cloudstack.storage.kv.actor.{BufferedHistoryKvActor, HistoricalKvActor, HistoryKvActor, KvActor}
import com.bwsw.cloudstack.storage.kv.manager.{ElasticsearchKvStorageManager, KvStorageManager}
import com.bwsw.cloudstack.storage.kv.processor.{ElasticsearchKvProcessor, KvProcessor}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import scaldi.Module

class KvStorageModule extends Module {

  val configuration = new Configuration

  bind[Configuration] toNonLazy configuration

  bind[HttpClient] toNonLazy HttpClient(ElasticsearchClientUri(configuration.getElasticsearchUri))
  bind[KvProcessor] to injected[ElasticsearchKvProcessor]
  bind[KvStorageManager] to injected[ElasticsearchKvStorageManager]
  bind[HistoryKvActor] to injected[BufferedHistoryKvActor]
  bind[KvActor] to injected[HistoricalKvActor]
}
