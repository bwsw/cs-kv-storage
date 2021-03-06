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
import akka.testkit.TestActorRef
import com.bwsw.cloudstack.storage.kv.entity.CheckName.{HistoryStorageTemplate, StorageRegistry, StorageTemplate}
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.request.HealthCheckRequest
import com.bwsw.cloudstack.storage.kv.message.response.{DetailedHealthCheckResponse, StatusHealthCheckResponse}
import com.bwsw.cloudstack.storage.kv.mock.MockActor
import com.bwsw.cloudstack.storage.kv.mock.MockActor.ResponsiveExpectation
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.duration._

class HealthServletSuite
  extends ScalatraSuite
  with FunSpecLike
  with MockFactory {

  private implicit val system: ActorSystem = ActorSystem()
  private val healthActor = TestActorRef(new MockActor())
  private val jsonError = s"""{\"status\":\"$Unhealthy\",\"checks\":[""" +
    s"""{\"name\":\"$StorageRegistry\",\"status\":\"$Unhealthy\",\"message\":\"$NotFound\"},""" +
    s"""{\"name\":\"$StorageTemplate\",\"status\":\"$Healthy\",\"message\":\"$Ok\"},""" +
    s"""{\"name\":\"$HistoryStorageTemplate\",\"status\":\"$Unhealthy\",\"message\":\"${ElasticsearchError()}\"}""" +
    "]}"
  private val jsonOk = s"""{\"status\":\"$Healthy\",\"checks\":[""" +
    s"""{\"name\":\"$StorageRegistry\",\"status\":\"$Healthy\",\"message\":\"$Ok\"},""" +
    s"""{\"name\":\"$StorageTemplate\",\"status\":\"$Healthy\",\"message\":\"$Ok\"},""" +
    s"""{\"name\":\"$HistoryStorageTemplate\",\"status\":\"$Healthy\",\"message\":\"$Ok\"}""" +
    "]}"

  describe("a HealthServlet") {
    addServlet(new HealthServlet(system, 1500.millis, healthActor), "/health/*")

    describe("check non detailed") {
      it("should return 200 Ok if the storage is running and set up properly") {
        healthActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(HealthCheckRequest(false), () => StatusHealthCheckResponse(Healthy)))
        get("/health") {
          status should equal(200)
        }
      }

      it("should return 500 Internal Server Error if the storage has problems") {
        healthActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            HealthCheckRequest(false),
            () => StatusHealthCheckResponse(Unhealthy)))
        get("/health") {
          status should equal(500)
        }
      }
    }

    describe("check detailed") {
      it("should return 200 Ok if all checks are healthy") {
        healthActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            HealthCheckRequest(true),
            () => DetailedHealthCheckResponse(
              Healthy, Seq(
                Check(StorageRegistry, Healthy, Ok),
                Check(StorageTemplate, Healthy, Ok),
                Check(HistoryStorageTemplate, Healthy, Ok)
              ))))
        get("/health?detailed=true") {
          status should equal(200)
          body should equal(jsonOk)
          response.getContentType should include("application/json")
        }
      }

      it("should return 500 Internal Server Error if the storage has problems") {
        healthActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            HealthCheckRequest(true),
            () => DetailedHealthCheckResponse(
              Unhealthy, Seq(
                Check(StorageRegistry, Unhealthy, NotFound),
                Check(StorageTemplate, Healthy, Ok),
                Check(HistoryStorageTemplate, Unhealthy, ElasticsearchError())
              ))))
        get("/health?detailed=true") {
          status should equal(500)
          body should equal(jsonError)
          response.getContentType should include("application/json")
        }
      }
    }

    describe("check bad request") {
      it("should return 400 Bad Request if detailed parameter value is invalid") {
        get("/health?detailed=bad") {
          status should equal(400)
          body should equal("")
        }
      }
    }
  }
}
