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

package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.Timers
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{RegistryFields, RegistryIndex}
import com.sksamuel.elastic4s.http.ElasticDsl.{clearScroll, searchScroll, _}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHits
import scaldi.Injector
import scaldi.akka.AkkaInjectable.inject

import scala.concurrent.Future

/** Actor responsible for continual cache update from Elasticsearch **/
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

  private var lastUpdateTimestamp = clock.currentTimeMillis

  timers.startPeriodicTimer(UpdateTimer, UpdateTimeout, appConfig.getCacheUpdateTime)

  override def receive: Receive = {
    case UpdateTimeout =>
      log.info("{}: update storage cache by timeout.", getClass.getName)
      update()
  }

  private def update(): Unit = {
    val updateStartTimestamp = clock.currentTimeMillis
    val keepAlive = elasticsearchConfig.getScrollKeepAlive
    client.execute {
      search(RegistryIndex)
        .query(rangeQuery(RegistryFields.LastUpdated).gte(lastUpdateTimestamp - appConfig.getRequestTimeout.toMillis))
        .size(elasticsearchConfig.getScrollPageSize)
        .scroll(keepAlive)
        .fetchSource(false)
    }.map {
      case Left(failure) =>
        log.error("Elasticsearch storage search request failed during cache update: {}", failure.error)
        cache.invalidateAll()
        lastUpdateTimestamp = updateStartTimestamp
      case Right(success) =>
        updateValues(success.result.hits)
        if (success.result.scrollId.nonEmpty)
          scrollAll(success.result.scrollId.get, keepAlive).onComplete(_ => lastUpdateTimestamp = updateStartTimestamp)
        else
          lastUpdateTimestamp = updateStartTimestamp
    }
  }

  private def scrollAll(
      scrollId: String,
      keepAlive: String): Future[Unit] = {
    client.execute(searchScroll(scrollId).keepAlive(keepAlive))
      .flatMap {
        case Left(failure) =>
          log.info("Elasticsearch scroll request failure during cache update: {}", failure.error)
          cache.invalidateAll()
          Future()
        case Right(success) =>
          if (success.result.hits.hits.length == 0) {
            client.execute {
              clearScroll(success.result.scrollId.get)
            }.map {
              case Left(failure) =>
                log.info("Elasticsearch clear scroll request failure during cache update: {}", failure.error)
                Future()
              case _ =>
            }
          } else {
            updateValues(success.result.hits)
            scrollAll(success.result.scrollId.get, keepAlive)
          }
      }
  }

  private def updateValues(searchHits: SearchHits): Unit = {
    cache.invalidateAll(searchHits.hits.map(_.id))
  }
}

object ElasticsearchCacheUpdateActor {

  private case object UpdateTimer

  private case object UpdateTimeout

}
