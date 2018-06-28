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
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.{Check, _}
import com.bwsw.cloudstack.storage.kv.message.request.{CheckTemplateExistsRequest, HealthCheckRequest}
import com.sksamuel.elastic4s.admin.IndicesExists
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import scaldi.{Injector, Module}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class ElasticsearchHealthActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit private val client: HttpClient = mock[HttpClient]

  private val checkTestProbe = TestProbe()
  private val appConf = mock[AppConfig]
  private val registryIndex = "storage-registry"
  private val storageTemplate = "storage"
  private val historyStorageTemplate = "history-storage"
  private val successMsg = ""
  private val failMsg = "Not found"
  private val exceptionMsg = "Elasticsearch error"
  private val timeout = 1000.millis

  class CheckActorMock(testProbe: TestProbe) extends CheckActor {
    override def receive: Receive = {
      case msg: Any =>
        testProbe.ref.forward(msg)
    }
  }


  describe("a ElasticsearchHealthActor") {
    (appConf.getRequestTimeout _).expects().returning(1.second)

    implicit val testModule: Injector =
      new Module {
        bind[AppConfig] toNonLazy appConf
        bind[CheckActor] toProvider new CheckActorMock(checkTestProbe)
        bind[HttpClient] to client
      }
    val elasticsearchHealthActor = system.actorOf(Props(new ElasticsearchHealthActor))

    def test(
        detailed: Boolean,
        healthStatus: HealthStatus,
        storageRegistryCheckResult: Either[Unit, Boolean] = Right(true),
        storageTemplateCheckResult: Boolean = true,
        historyStorageTemplateCheckResult: Boolean = true): HealthCheckResponseBody = {

      val storageRegistryCheck = storageRegistryCheckResult match {
        case Right(true) =>
          expectStorageRegistryExists()
          Check(StorageRegistry, Healthy, successMsg)
        case Right(false) =>
          expectStorageRegistryNotFound()
          Check(StorageRegistry, Unhealthy, failMsg)
        case Left(_) =>
          expectStorageRegistryRequestFailure(exceptionMsg)
          Check(StorageRegistry, Unhealthy, exceptionMsg)
      }

      elasticsearchHealthActor ! HealthCheckRequest(detailed)
      checkTestProbe.expectMsg(timeout, CheckTemplateExistsRequest(storageTemplate))
      checkTestProbe.reply(storageTemplateCheckResult)
      checkTestProbe.expectMsg(timeout, CheckTemplateExistsRequest(historyStorageTemplate))
      checkTestProbe.reply(historyStorageTemplateCheckResult)

      if (detailed)
        expectMsg(
          timeout, HealthCheckDetailedResponseBody(
            healthStatus, Seq(
              storageRegistryCheck,
              Check(
                StorageTemplate,
                if (storageTemplateCheckResult) Healthy else Unhealthy,
                if (storageTemplateCheckResult) successMsg else failMsg),
              Check(
                HistoryStorageTemplate,
                if (historyStorageTemplateCheckResult) Healthy else Unhealthy,
                if (historyStorageTemplateCheckResult) successMsg else failMsg))))
      else
        expectMsg(timeout, HealthCheckShortResponseBody(healthStatus))
    }

    describe("HealthCheckRequest(detailed = false)") {

      it("should report Healthy if all checks passed") {
        test(detailed = false, Healthy)
      }

      it("should report Unhealthy if at least storage template check returned false") {
        test(detailed = false, Unhealthy, storageTemplateCheckResult = false)
      }

      it("should report Unhealthy if at least history storage template check returned false") {
        test(detailed = false, Unhealthy, historyStorageTemplateCheckResult = false)
      }

      it("should report Unhealthy if at least storage registry check failed") {
        test(detailed = false, Unhealthy, storageRegistryCheckResult = Left())
      }

      it("should report Unhealthy if at least storage registry check returned false") {
        test(detailed = false, Unhealthy, storageRegistryCheckResult = Right(false))
      }
    }

    describe("HealthCheckRequest(detailed = true)") {

      it("should return Healthy detailed report if all checks passed") {
        test(detailed = true, Healthy)
      }

      it("should return Unhealthy detailed report if at least storage template check returned false") {
        test(detailed = true, Unhealthy, storageTemplateCheckResult = false)
      }

      it("should return Unhealthy detailed report if at least history storage template check returned false") {
        test(detailed = true, Unhealthy, historyStorageTemplateCheckResult = false)
      }

      it("should report Unhealthy if at least storage registry check failed") {
        test(detailed = true, Unhealthy, storageRegistryCheckResult = Left())
      }

      it("should return Unhealthy detailed report if at least storage registry check returned false") {
        test(detailed = true, Unhealthy, storageRegistryCheckResult = Right(false))
      }
    }
  }

  private def expectStorageRegistryExists() =
    expectIndexExistsRequest().returning(getRequestSuccessFuture(IndexExistsResponse(true)))

  private def expectStorageRegistryNotFound() =
    expectIndexExistsRequest().returning(getRequestSuccessFuture(IndexExistsResponse(false)))

  private def expectStorageRegistryRequestFailure(error: String) =
    expectIndexExistsRequest().returning(Future(Left(
      RequestFailure(500, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException(error))))))

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] =
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))

  private def expectIndexExistsRequest()(implicit client: HttpClient) =
    (client.execute[IndicesExists, IndexExistsResponse]
      (_: IndicesExists)
      (_: HttpExecutable[IndicesExists, IndexExistsResponse], _: ExecutionContext))
      .expects(indexExists(registryIndex), IndexExistsHttpExecutable, *)

}
