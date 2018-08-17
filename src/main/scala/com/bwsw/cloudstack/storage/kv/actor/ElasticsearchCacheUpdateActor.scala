package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{RegistryFields, RegistryIndex}
import com.sksamuel.elastic4s.http.ElasticDsl.{clearScroll, searchScroll, _}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHits
import scaldi.Injector
import scaldi.akka.AkkaInjectable.inject

class ElasticsearchCacheUpdateActor(implicit inj: Injector)
  extends CacheUpdateActor
  with Timers
  with akka.actor.ActorLogging {

  import ElasticsearchCacheUpdateActor._
  import context.dispatcher

  private val appConfig = inject[AppConfig]
  private val cache = inject[StorageCache]
  private val client = inject[HttpClient]
  private val clock = inject[Clock]
  private val elasticsearchConfig = inject[ElasticsearchConfig]

  private var lastUpdateTimestamp = 0L

  timers.startPeriodicTimer(UpdateTimer, UpdateTimeout, appConfig.getCacheUpdateTime)


  override def receive: Receive = {
    case UpdateTimeout =>
      log.info("{}: update storage cache by timeout.", getClass.getName)
      update()
  }

  private def update(): Unit = {
    if (lastUpdateTimestamp == 0) {
      lastUpdateTimestamp = clock.currentTimeMillis
    } else {
      val keepAlive = elasticsearchConfig.getScrollKeepAlive
      client.execute {
        search(RegistryIndex).query(rangeQuery(RegistryFields.LastUpdated).gte(lastUpdateTimestamp))
          .size(elasticsearchConfig.getScrollPageSize)
          .scroll(keepAlive)
      }.map {
        case Left(failure) =>
          log.info("Elasticsearch storage update request failure: {}", failure.error)
        //TODO: handle failure
        case Right(success) =>
          if (success.result.scrollId.nonEmpty)
            scrollAll(success.result.scrollId.get, keepAlive)
          updateValues(success.result.hits)
      }
    }
  }

  private def scrollAll(
      scrollId: String,
      keepAlive: String): Unit = {
    client.execute(searchScroll(scrollId).keepAlive(keepAlive))
      .map {
        case Left(failure) =>
          log.info("Elasticsearch scroll request failure: {}", failure.error)
        //TODO: handle failure
        case Right(success) =>
          if (success.result.hits.hits.length == 0) {
            client.execute {
              clearScroll(success.result.scrollId.get)
            }
          } else {
            updateValues(success.result.hits)
            scrollAll(success.result.scrollId.get, keepAlive)
          }
      }
  }

  private def updateValues(searchHits: SearchHits): Unit = cache.updateAll(searchHits.hits.map {
    hit =>
      val id = hit.id
      val source = hit.sourceAsMap
      if (!getValue[Boolean](source, RegistryFields.Deleted)) {
        id -> Some(Storage(
          id,
          getValue(source, RegistryFields.Type),
          getValue(source, RegistryFields.HistoryEnabled),
          getValue[String](source, RegistryFields.SecretKey).toCharArray))
      } else {
        id -> None
      }
  }.toMap)
}

object ElasticsearchCacheUpdateActor {

  private case object UpdateTimer

  private case object UpdateTimeout

  private def getValue[T](source: Map[String, Any], key: String): T = source.get(key) match {
    case Some(s) => s.asInstanceOf[T]
    case None => throw new RuntimeException("Invalid result") //TODO: handle failure
  }
}
