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

package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.processor.HealthProcessor
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.Future

class HealthServletSuite
  extends ScalatraSuite
    with FunSpecLike
    with MockFactory {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val system = ActorSystem()
  private val healthProcessor = mock[HealthProcessor]
  private val jsonError = "{\"status\":\"UNHEALTHY\",\"checks\":[" +
    "{\"name\":\"STORAGE_REGISTRY\",\"status\":\"UNHEALTHY\",\"message\":\"Not found\"}," +
    "{\"name\":\"STORAGE_TEMPLATE\",\"status\":\"HEALTHY\",\"message\":\"\"}," +
    "{\"name\":\"HISTORY_STORAGE_TEMPLATE\",\"status\":\"UNHEALTHY\",\"message\":\"ElasticsearchError\"}" +
    "]}"
  private val jsonOk = "{\"status\":\"HEALTHY\",\"checks\":[" +
    "{\"name\":\"STORAGE_REGISTRY\",\"status\":\"HEALTHY\",\"message\":\"\"}," +
    "{\"name\":\"STORAGE_TEMPLATE\",\"status\":\"HEALTHY\",\"message\":\"\"}," +
    "{\"name\":\"HISTORY_STORAGE_TEMPLATE\",\"status\":\"HEALTHY\",\"message\":\"\"}" +
    "]}"

  describe("a HealthServlet") {
    addServlet(new HealthServlet(system, healthProcessor), "/health/*")

    describe("check non detailed") {
      it("should return 200 Ok if storage is running and set up properly") {
        (healthProcessor.check _).expects().returning(Future(Healthy)).once
        get("/health") {
          status should equal(200)
        }
      }

      it("should return 500 Internal Server Error if storage have problems") {
        (healthProcessor.check _).expects().returning(Future(Unhealthy)).once
        get("/health") {
          status should equal(500)
        }
      }

      it("should return 500 Internal Server Error if storage check requests had errors") {
        (healthProcessor.check _).expects().returning(Future(Unhealthy)).once
        get("/health") {
          status should equal(500)
        }
      }
    }

    describe("check detailed") {
      it("should return 200 Ok and all checks should be healthy") {
        (healthProcessor.checkDetailed _).expects().returning(Future(HealthResponseBody(Healthy, Seq(
          Check(StorageRegistry, Healthy, ""),
          Check(StorageTemplate, Healthy, ""),
          Check(HistoryStorageTemplate, Healthy, "")
        )))).once
        get("/health?detailed=true") {
          status should equal(200)
          body should equal(jsonOk)
        }
      }

      it("should return 500 Internal Server Error if storage have problems") {
        (healthProcessor.checkDetailed _).expects().returning(Future(HealthResponseBody(Unhealthy, Seq(
          Check(StorageRegistry, Unhealthy, "Not found"),
          Check(StorageTemplate, Healthy, ""),
          Check(HistoryStorageTemplate, Unhealthy, "ElasticsearchError")
        )))).once
        get("/health?detailed=true") {
          status should equal(500)
          body should equal(jsonError)
        }
      }
    }
  }
}
