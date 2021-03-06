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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.bwsw.cloudstack.storage.kv.entity.HealthStatus.{Healthy, Unhealthy}
import com.bwsw.cloudstack.storage.kv.entity.{CheckName, HealthStatus, StatusMessage}
import com.bwsw.cloudstack.storage.kv.message.request.HealthCheckRequest
import com.bwsw.cloudstack.storage.kv.message.response.{DetailedHealthCheckResponse, StatusHealthCheckResponse}
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class HealthServlet(system: ActorSystem, requestTimeout: FiniteDuration, healthActor: ActorRef)
  extends ScalatraServlet
  with FutureSupport
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats
    .preservingEmptyValues + new HealthStatusSerializer + new NameSerializer + new StatusMessageSerializer

  protected implicit val akkaTimeout: Timeout = requestTimeout

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/") {
    new AsyncResult() {
      val is: Future[_] = {
        try {
          val detailed = params.getOrElse("detailed", "false").toBoolean
          (healthActor ? HealthCheckRequest(detailed)).map {
            case StatusHealthCheckResponse(Healthy) => Ok("")
            case StatusHealthCheckResponse(Unhealthy) => InternalServerError("")
            case detailedResponse: DetailedHealthCheckResponse =>
              contentType = formats("json")
              detailedResponse.status match {
                case Healthy => Ok(detailedResponse)
                case Unhealthy => InternalServerError(detailedResponse)
              }
          }
        }
        catch {
          case iae: IllegalArgumentException => Future(BadRequest(""))
        }
      }
    }
  }

  private class StatusMessageSerializer extends CustomSerializer[StatusMessage](
    format => ( {
      case JString(s) => throw new UnsupportedOperationException()
    }, {
      case sm: StatusMessage => JString(sm.toString)
    }))

  private class HealthStatusSerializer extends CustomSerializer[HealthStatus](
    format => ( {
      case JString(s) => HealthStatus.parse(s)
    }, {
      case hs: HealthStatus => JString(hs.toString)
    }))

  private class NameSerializer extends CustomSerializer[CheckName](
    format => ( {
      case JString(s) => CheckName.parse(s)
    }, {
      case op: CheckName => JString(op.toString)
    }))

}
