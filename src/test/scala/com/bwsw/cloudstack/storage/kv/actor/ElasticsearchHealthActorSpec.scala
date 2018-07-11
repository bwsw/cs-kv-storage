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
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.CheckName.{HistoryStorageTemplate, StorageRegistry, StorageTemplate}
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity.{Check, _}
import com.bwsw.cloudstack.storage.kv.message.request.{HealthCheckRequest, TemplateCheckRequest}
import com.bwsw.cloudstack.storage.kv.message.response.{DetailedHealthCheckResponse, HealthCheckResponse,
  StatusHealthCheckResponse}
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.{DataStorageTemplateName, HistoryStorageTemplateName, RegistryIndex}
import com.sksamuel.elastic4s.admin.IndicesExists
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.index.admin.IndexExistsResponse
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import scaldi.{Injector, Module}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHealthActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit private val client: HttpClient = mock[HttpClient]

  private val checkTestProbe = TestProbe()
  private val appConf = mock[AppConfig]
  private val timeout = 1000.millis

  class TemplateCheckActorMock(testProbe: TestProbe) extends TemplateCheckActor {
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
        bind[TemplateCheckActor] toProvider new TemplateCheckActorMock(checkTestProbe)
        bind[HttpClient] to client
      }
    val elasticsearchHealthActor = system.actorOf(Props(new ElasticsearchHealthActor))

    def test(
        healthStatus: HealthStatus,
        storageRegistryCheckResult: Either[Unit, Boolean] = Right(true),
        storageTemplateCheckResult: Boolean = true,
        historyStorageTemplateCheckResult: Boolean = true): HealthCheckResponse = {

      val storageRegistryCheck = storageRegistryCheckResult match {
        case Right(true) =>
          expectStorageRegistryExists()
          Check(StorageRegistry, Healthy, Ok)
        case Right(false) =>
          expectStorageRegistryNotFound()
          Check(StorageRegistry, Unhealthy, NotFound)
        case Left(_) =>
          expectStorageRegistryRequestFailure(ElasticsearchError().toString)
          Check(StorageRegistry, Unhealthy, ElasticsearchError())
      }

      val storageCheck =
        if (storageTemplateCheckResult)
          Check(StorageTemplate, Healthy, Ok)
        else
          Check(StorageTemplate, Unhealthy, NotFound)

      val historyStorageCheck =
        if (historyStorageTemplateCheckResult)
          Check(HistoryStorageTemplate, Healthy, Ok)
        else
          Check(HistoryStorageTemplate, Unhealthy, NotFound)

      elasticsearchHealthActor ! HealthCheckRequest(true)
      checkTestProbe.expectMsg(timeout, TemplateCheckRequest(DataStorageTemplateName, StorageTemplate))
      checkTestProbe.reply(storageCheck)
      checkTestProbe
        .expectMsg(timeout, TemplateCheckRequest(HistoryStorageTemplateName, HistoryStorageTemplate))
      checkTestProbe.reply(historyStorageCheck)

      expectMsg(
        timeout, DetailedHealthCheckResponse(
          healthStatus, Seq(
            storageRegistryCheck,
            storageCheck,
            historyStorageCheck)))
    }

    describe("HealthCheckRequest(detailed = false)") {

      it("should report Healthy") {
        elasticsearchHealthActor ! HealthCheckRequest(false)
        expectMsg(timeout, StatusHealthCheckResponse(Healthy))
      }

    }

    describe("HealthCheckRequest(detailed = true)") {

      it("should return Healthy detailed response if all checks passed") {
        test(Healthy)
      }

      it("should return Unhealthy detailed response if the storage template check returns false") {
        test(Unhealthy, storageTemplateCheckResult = false)
      }

      it("should return Unhealthy detailed response if the history storage template check returns false") {
        test(Unhealthy, historyStorageTemplateCheckResult = false)
      }

      it("should report Unhealthy detailed response if the storage registry check fails") {
        test(Unhealthy, storageRegistryCheckResult = Left())
      }

      it("should return Unhealthy detailed response if the storage registry check returns false") {
        test(Unhealthy, storageRegistryCheckResult = Right(false))
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

  private def expectIndexExistsRequest()(implicit client: HttpClient) =
    (client.execute[IndicesExists, IndexExistsResponse]
      (_: IndicesExists)
      (_: HttpExecutable[IndicesExists, IndexExistsResponse], _: ExecutionContext))
      .expects(indexExists(RegistryIndex), IndexExistsHttpExecutable, *)

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] =
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))

}
