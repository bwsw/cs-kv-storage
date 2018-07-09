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
import scaldi.Module

class KvStorageModule extends Module {

  val appConfig = new AppConfig
  val elasticsearchConfig = new ElasticsearchConfig
  val clock = new Clock

  implicit val actorSystem: ActorSystem = ActorSystem("cs-kv-storage")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  bind[Clock] toNonLazy clock
  bind[AppConfig] toNonLazy appConfig
  bind[ElasticsearchConfig] toNonLazy elasticsearchConfig

  bind[ActorSystem] toNonLazy actorSystem
  bind[Materializer] toNonLazy actorMaterializer

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
