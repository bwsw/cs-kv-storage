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

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{DocumentType, RegistryIndex, StorageType}
import com.bwsw.cloudstack.storage.kv.util.test._
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.{ClearScrollResponse, SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.http.{HttpClient, HttpExecutable, Shards}
import com.sksamuel.elastic4s.searches.{ClearScrollDefinition, SearchDefinition, SearchScrollDefinition}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import scaldi.{Injector, Module}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

class ElasticsearchCacheUpdateActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with Eventually
  with FunSpecLike
  with Matchers
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val appConfig = mock[AppConfig]
  private val clock = mock[Clock]
  private val elasticsearchConfig = mock[ElasticsearchConfig]
  private implicit val httpClient: HttpClient = mock[HttpClient]
  private val storageCache = mock[StorageCache]

  private val updateTimeout = 500.millis
  private val timestamp = System.currentTimeMillis()
  private val timeout = 3.seconds
  private val scrollKeepAlive = "1m"
  private val scrollPageSize = 1000
  private val scrollId = Some("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ==")

  private val storage1 = Storage("storage-1", StorageType.Account, historyEnabled = true, "secret")
  private val storage2 = Storage("storage-2", StorageType.Temporary, historyEnabled = false, "secret")

  describe("an ElasticsearchCacheUpdateActor") {
    implicit val testModule: Injector = new Module {
      bind[AppConfig] to appConfig
      bind[Clock] to clock
      bind[ElasticsearchConfig] to elasticsearchConfig
      bind[HttpClient] to httpClient
      bind[StorageCache] to storageCache
    }

    describe("cache update by timer") {
      it("should update cache in one request") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        (clock.currentTimeMillis _).expects().returning(timestamp)
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))

        val updateTimestamp = Promise[Boolean]
        expectSearch().returning(getRequestSuccessFuture(getSearchResponse(List(storage1.uuid), 1)))
        (storageCache.invalidateAll(_: Iterable[String])).expects(Seq(storage1.uuid)).onCall {
          _: Iterable[String] =>
            updateTimestamp.success(true)
            ()
        }
        eventually(timeout(scaled(updateTimeout * 1.9))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }

      it("should update cache using scroll") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        (clock.currentTimeMillis _).expects().returning(timestamp)
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))

        val updateTimestamp = Promise[Boolean]
        expectSearch()
          .returning(getRequestSuccessFuture(getSearchResponse(List(storage1.uuid), 2, scrollId)))
        (storageCache.invalidateAll(_: Iterable[String])).expects(List(storage1.uuid))

        val scrollDefinition = searchScroll(scrollId.get, scrollKeepAlive)
        expectScroll(scrollDefinition)
          .returning(getRequestSuccessFuture(getSearchResponse(List(storage2.uuid), 2, scrollId)))
        expectScroll(scrollDefinition).returning(getRequestSuccessFuture(getSearchResponse(List(), 2, scrollId)))
        expectClearScroll(scrollId.get)
        (storageCache.invalidateAll(_: Iterable[String])).expects(List(storage2.uuid)).onCall {
          _: Iterable[String] =>
            updateTimestamp.success(true)
            ()
        }
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }

      it("should invalidate cache if update fails") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        (clock.currentTimeMillis _).expects().returning(timestamp)
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))

        val updateTimestamp = Promise[Boolean]
        expectSearch().returning(getRequestFailureFuture())
        (storageCache.invalidateAll: () => Unit).expects().onCall { () =>
          updateTimestamp.success(true)
        }
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }

      it("should invalidate cache if update fails on scroll") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        (clock.currentTimeMillis _).expects().returning(timestamp)
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))

        val updateTimestamp = Promise[Boolean]
        expectSearch()
          .returning(getRequestSuccessFuture(getSearchResponse(List(storage1.uuid), 2, scrollId)))
        (storageCache.invalidateAll(_: Iterable[String])).expects(List(storage1.uuid))

        val scrollDefinition = searchScroll(scrollId.get, scrollKeepAlive)
        expectScroll(scrollDefinition).returning(getRequestFailureFuture())
        (storageCache.invalidateAll: () => Unit).expects().onCall { () =>
          updateTimestamp.success(true)
        }
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }
    }
  }

  private def expectSearch()(implicit client: HttpClient) = {
    (clock.currentTimeMillis _).expects().returning(timestamp)
    (appConfig.getRequestTimeout _).expects().returning(timeout)
    (elasticsearchConfig.getScrollKeepAlive _).expects().returning(scrollKeepAlive)
    (elasticsearchConfig.getScrollPageSize _).expects().returning(scrollPageSize)
    (client.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(
      _: HttpExecutable[SearchDefinition, SearchResponse],
      _: ExecutionContext))
      .expects(*, SearchHttpExecutable, *)
  }

  private def expectScroll(searchScrollDefinition: SearchScrollDefinition)(implicit client: HttpClient) =
    (client.execute[SearchScrollDefinition, SearchResponse](_: SearchScrollDefinition)(
      _: HttpExecutable[SearchScrollDefinition, SearchResponse],
      _: ExecutionContext))
      .expects(searchScrollDefinition, SearchScrollHttpExecutable, *)

  private def expectClearScroll(scrollId: String)(implicit client: HttpClient) =
    (client.execute[ClearScrollDefinition, ClearScrollResponse](_: ClearScrollDefinition)(
      _: HttpExecutable[ClearScrollDefinition, ClearScrollResponse],
      _: ExecutionContext))
      .expects(clearScroll(scrollId), ClearScrollHttpExec, *)


  private def getSearchResponse(
      registry: List[String],
      total: Int,
      scrollId: Option[String] = None) = SearchResponse(
    1,
    isTimedOut = false,
    isTerminatedEarly = false,
    Map.empty,
    Shards(1, 0, 1),
    scrollId,
    Map.empty,
    SearchHits(
      total,
      1,
      registry.map(getSearchHit).toArray))

  private def getSearchHit(id: String) =
    SearchHit(
      id,
      RegistryIndex,
      DocumentType,
      1,
      1,
      None,
      None,
      None,
      None,
      None,
      None,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty)

}
