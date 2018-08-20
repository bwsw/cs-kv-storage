package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.entity.{History, Storage}
import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{
  DocumentType, HistoryFields, RegistryFields, RegistryIndex,
  StorageType
}
import com.sksamuel.elastic4s.http.ElasticDsl.{rangeQuery, search, _}
import com.sksamuel.elastic4s.http.{HttpClient, HttpExecutable, Shards}
import com.sksamuel.elastic4s.http.search.{ClearScrollResponse, SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.searches.{ClearScrollDefinition, SearchDefinition, SearchScrollDefinition}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import scaldi.{Injector, Module}
import com.bwsw.cloudstack.storage.kv.util.test._

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._

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
  private val scrollKeepAlive = "1m"
  private val scrollPageSize = 1000
  private val scrollId = Some("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ==")

  private val storage1 = Storage("storage-1", StorageType.Account, historyEnabled = true, "secret".toCharArray)
  private val storage1Map = Map[String, AnyRef](
    RegistryFields.SecretKey -> storage1.secretKey.mkString,
    RegistryFields.LastUpdated -> timestamp.asInstanceOf[AnyRef],
    RegistryFields.Deleted -> false.asInstanceOf[AnyRef],
    RegistryFields.Type -> storage1.storageType,
    RegistryFields.HistoryEnabled -> storage1.historyEnabled.asInstanceOf[AnyRef]
  )
  private val storage2 = Storage("storage-2", StorageType.Temporary, historyEnabled = false, "secret".toCharArray)
  private val storage2Map = Map[String, AnyRef](
    RegistryFields.SecretKey -> storage2.secretKey.mkString,
    RegistryFields.LastUpdated -> timestamp.asInstanceOf[AnyRef],
    RegistryFields.Deleted -> false.asInstanceOf[AnyRef],
    RegistryFields.Type -> storage2.storageType,
    RegistryFields.HistoryEnabled -> storage2.historyEnabled.asInstanceOf[AnyRef]
  )

  describe("an ElasticsearchCacheUpdateActor") {
    implicit val testModule: Injector = new Module {
      bind[AppConfig] to appConfig
      bind[Clock] to clock
      bind[ElasticsearchConfig] to elasticsearchConfig
      bind[HttpClient] to httpClient
      bind[StorageCache] to storageCache
    }


    describe("initial tick of the timer") {
      it("should only update lastUpdateTimestamp on first tick") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        val promise = Promise[Boolean]
        (clock.currentTimeMillis _).expects().onCall { () =>
          promise.success(true)
          timestamp
        }
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          promise.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }

    }

    describe("timed cache update") {
      it("should update cache in one request") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        val initTimestamp = Promise[Boolean]
        (clock.currentTimeMillis _).expects().onCall { () =>
          initTimestamp.success(true)
          timestamp
        }
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          initTimestamp.isCompleted should be(true)
        }
        val updateTimestamp = Promise[Boolean]
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(scrollKeepAlive)
        (elasticsearchConfig.getScrollPageSize _).expects().returning(scrollPageSize)
        expectSearch(search(RegistryIndex).query(rangeQuery(RegistryFields.LastUpdated).gte(timestamp))
                       .size(scrollPageSize)
                       .scroll(scrollKeepAlive))
          .returning(getRequestSuccessFuture(getSearchResponse(Map(storage1.uuid -> storage1Map), 1)))
        (storageCache.updateAll _).expects(Map(storage1.uuid -> Some(storage1)))
        (clock.currentTimeMillis _).expects().onCall { () =>
          updateTimestamp.success(true)
          timestamp
        }
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }

      it("should update cache using scroll") {
        (appConfig.getCacheUpdateTime _).expects().returning(updateTimeout)
        val initTimestamp = Promise[Boolean]
        (clock.currentTimeMillis _).expects().onCall { () =>
          initTimestamp.success(true)
          timestamp
        }
        val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          initTimestamp.isCompleted should be(true)
        }
        val updateTimestamp = Promise[Boolean]
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(scrollKeepAlive)
        (elasticsearchConfig.getScrollPageSize _).expects().returning(scrollPageSize)
        expectSearch(search(RegistryIndex).query(rangeQuery(RegistryFields.LastUpdated).gte(timestamp))
                       .size(scrollPageSize)
                       .scroll(scrollKeepAlive))
          .returning(getRequestSuccessFuture(getSearchResponse(Map(storage1.uuid -> storage1Map), 2, scrollId)))
        (storageCache.updateAll _).expects(Map(storage1.uuid -> Some(storage1)))

        val scrollDefinition = searchScroll(scrollId.get, scrollKeepAlive)
        expectScroll(scrollDefinition)
          .returning(getRequestSuccessFuture(getSearchResponse(Map(storage2.uuid -> storage2Map), 2, scrollId)))
        (storageCache.updateAll _).expects(Map(storage2.uuid -> Some(storage2)))
        expectScroll(scrollDefinition).returning(getRequestSuccessFuture(getSearchResponse(Map(), 2, scrollId)))
        expectClearScroll(scrollId.get)
        (clock.currentTimeMillis _).expects().onCall { () =>
          updateTimestamp.success(true)
          timestamp
        }
        eventually(timeout(scaled(updateTimeout * 1.4))) {
          updateTimestamp.isCompleted should be(true)
        }
        system.stop(elasticsearchCacheUpdateActor)
      }
    }

  }

  private def expectSearch(searchDefinition: SearchDefinition)(implicit client: HttpClient) = {
    (client.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(
      _: HttpExecutable[SearchDefinition, SearchResponse],
      _: ExecutionContext))
      .expects(searchDefinition, SearchHttpExecutable, *)
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
      registry: Map[String, Map[String, AnyRef]],
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
      registry.map { case (uuid, source) => getSearchHit(uuid, source) }.toArray))

  private def getSearchHit(id: String, source: Map[String, AnyRef]) = {
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
      source,
      Map.empty,
      Map.empty,
      Map.empty)
  }

}
