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
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.{ImplicitSender, TestKit}
import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.entity.CheckName.StorageTemplate
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.request.TemplateCheckRequest
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterEach, FunSpecLike}
import scaldi.{Injector, Module}

import scala.concurrent.duration._

class ElasticsearchTemplateCheckActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterEach {

  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))

  private val esConfig = mock[ElasticsearchConfig]
  private val uriBase = "http://localhost:"
  private val name = "someTemplate"
  private val unexpectedMsg = "Unexpected status: "
  private val checkName = StorageTemplate
  private val templatePath = "/_template/" + name
  private val wireMockServer = new WireMockServer(wireMockConfig().dynamicPort())

  override def beforeEach {
    wireMockServer.start()
  }

  override def afterEach {
    wireMockServer.stop()
  }

  describe("a ElasticsearchCheckActor") {

    implicit val testModule: Injector =
      new Module {
        bind[ElasticsearchConfig] toNonLazy esConfig
      }

    val elasticsearchCheckActor = system.actorOf(Props(new ElasticsearchTemplateCheckActor))

    def test(status: Int, msg: Check, timeout: FiniteDuration) = {
      (esConfig.getUri _).expects().returning(uriBase + wireMockServer.port())
      wireMockServer.stubFor(
        head(urlPathEqualTo(templatePath))
          .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json; charset=UTF-8")
                        .withStatus(status)))
      elasticsearchCheckActor ! TemplateCheckRequest(name, checkName)
      expectMsg(timeout, msg)
    }

    it("should return true if the template exists") {
      test(200, Check(checkName, Healthy, Ok), 5000.millis)
    }

    it("should return false if the template does not exist") {
      test(404, Check(checkName, Unhealthy, NotFound), 1500.millis)
    }

    it("should return false if the request processing fails") {
      test(500, Check(checkName, Unhealthy, Unexpected(unexpectedMsg + 500)), 1500.millis)
    }
  }

}
