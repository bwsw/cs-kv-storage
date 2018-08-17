package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.util.Clock
import com.sksamuel.elastic4s.http.HttpClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}

class ElasticsearchCacheUpdateActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val appConfig = mock[AppConfig]
  private val clock = mock[Clock]
  private val elasticsearchConfig = mock[ElasticsearchConfig]
  private val httpClient = mock[HttpClient]
  private val storageCache = mock[StorageCache]

  describe("an ElasticsearchCacheUpdateActor") {
    val elasticsearchCacheUpdateActor = system.actorOf(Props(new ElasticsearchCacheUpdateActor))

    describe("initial tick of the timer")

    describe("timed cache update") {
      Thread.sleep(1000)
    }
  }
}
