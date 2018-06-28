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
import com.bwsw.cloudstack.storage.kv.message.request.CheckTemplateExistsRequest
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.client.WireMock._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterEach, FunSpecLike}
import scaldi.{Injector, Module}

import scala.concurrent.duration._

class ElasticsearchCheckActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterEach {

  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))
  
  private val esConfig = mock[ElasticsearchConfig]
  private val uriBase = "http://localhost:"
  private val name = "someTemplate"
  private val timeout = 2500.millis
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

    val elasticsearchCheckActor = system.actorOf(Props(new ElasticsearchCheckActor))

    def test(status: Int, msg: Boolean) = {
      (esConfig.getUri _).expects().returning(uriBase + wireMockServer.port())
      wireMockServer.stubFor(
        head(urlPathEqualTo(templatePath))
          .willReturn(aResponse()
            .withHeader("Content-Type", "application/json; charset=UTF-8")
            .withStatus(status)))
      elasticsearchCheckActor ! CheckTemplateExistsRequest(name)
      expectMsg(timeout, msg)
    }

    it("should return true if template exists") {
      test(200, msg = true)
    }

    it("should return false if template does not exist") {
      test(400, msg = false)
    }

    it("should return false if request processing failed") {
      test(500, msg = false)
    }
  }

}
