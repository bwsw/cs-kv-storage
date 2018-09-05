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
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, ConflictError, NotFoundError, UnauthorizedError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvMultiGetRequest, _}
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.SecretKeyHeader
import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, Formats, JObject, MappingException}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class KvStorageServlet(system: ActorSystem, requestTimeout: FiniteDuration, kvProcessor: KvProcessor, kvActor: ActorRef)
  extends ScalatraServlet
  with FutureSupport
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues
  protected implicit val akkaTimeout: Timeout = requestTimeout

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/get/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty)
          (kvActor ? KvGetRequest(
            params("storage_uuid"),
            request.getHeader(SecretKeyHeader),
            params("key")))
            .map {
              case Right(value) =>
                contentType = formats("txt")
                value
              case Left(_: NotFoundError) => NotFound("")
              case Left(_: UnauthorizedError) => Unauthorized("")
              case _ => InternalServerError()
            }
        else
          Future(Unauthorized(""))
    }
  }

  post("/get/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty)
          if (request.getHeader("Content-Type") == formats("json"))
            parsedBody match {
              case json: JArray =>
                try {
                  val result = kvActor ? KvMultiGetRequest(
                    params("storage_uuid"),
                    request.getHeader(SecretKeyHeader),
                    json.extract[List[String]])
                  result.map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case Left(_: NotFoundError) => NotFound("")
                    case Left(_: UnauthorizedError) => Unauthorized("")
                    case _ => InternalServerError()
                  }
                } catch {
                  case ma: MappingException => Future(BadRequest())
                }
              case _ => Future(BadRequest())
            }
          else
            Future(BadRequest())
        else
          Future(Unauthorized(""))

    }
  }

  put("/set/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty)
          if (request.getHeader("Content-Type") == formats("txt")) {
            val result = kvActor ? KvSetRequest(
              params("storage_uuid"),
              request.getHeader(SecretKeyHeader),
              params("key"), request.body)
            result.map {
              case Right(_) => Ok()
              case Left(_: BadRequestError) => BadRequest()
              case Left(_: NotFoundError) => NotFound("")
              case Left(_: UnauthorizedError) => Unauthorized("")
              case _ => InternalServerError()
            }
          }
          else
            Future(BadRequest())
        else
          Future(Unauthorized(""))

    }
  }

  put("/set/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty)
          if (request.getHeader("Content-Type") == formats("json"))
            parsedBody match {
              case json: JObject =>
                try {
                  val result = kvActor ? KvMultiSetRequest(
                    params("storage_uuid"),
                    request.getHeader(SecretKeyHeader),
                    json.extract[Map[String, String]])
                  result.map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case Left(_: NotFoundError) => NotFound("")
                    case Left(_: UnauthorizedError) => Unauthorized("")
                    case _ => InternalServerError()
                  }
                } catch {
                  case e: MappingException => Future(BadRequest())
                }
              case _ => Future(BadRequest())
            }
          else
            Future(BadRequest())
        else
          Future(Unauthorized(""))
    }
  }

  delete("/delete/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty) {
          val result = kvActor ? KvDeleteRequest(
            params("storage_uuid"),
            request.getHeader(SecretKeyHeader),
            params("key"))
          result.map {
            case Right(_) => Ok()
            case Left(_: NotFoundError) => NotFound("")
            case Left(_: UnauthorizedError) => Unauthorized("")
            case _ => InternalServerError()
          }
        }
        else
          Future(Unauthorized(""))
    }
  }

  post("/delete/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty)
          if (request.getHeader("Content-Type") == formats("json"))
            parsedBody match {
              case json: JArray =>
                try {
                  val result = kvActor ? KvMultiDeleteRequest(
                    params("storage_uuid"),
                    request.getHeader(SecretKeyHeader),
                    json.extract[List[String]])
                  result.map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case Left(_: NotFoundError) => NotFound("")
                    case Left(_: UnauthorizedError) => Unauthorized("")
                    case _ => InternalServerError()
                  }
                } catch {
                  case e: MappingException => Future(BadRequest())
                }
              case _ => Future(BadRequest())
            }
          else
            Future(BadRequest())
        else
          Future(Unauthorized(""))
    }
  }

  get("/list/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty) {
          val result = kvActor ? KvListRequest(params("storage_uuid"), request.getHeader(SecretKeyHeader))
          result.map {
            case Right(value) =>
              contentType = formats("json")
              value
            case Left(_: NotFoundError) => NotFound("")
            case Left(_: UnauthorizedError) => Unauthorized("")
            case _ => InternalServerError()
          }
        }
        else
          Future(Unauthorized(""))
    }
  }

  post("/clear/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.header(SecretKeyHeader).nonEmpty) {
          val result = kvActor ? KvClearRequest(params("storage_uuid"), request.getHeader(SecretKeyHeader))
          result.map {
            case Right(_) => Ok()
            case Left(_: NotFoundError) => NotFound("")
            case Left(_: ConflictError) => Conflict("")
            case Left(_: UnauthorizedError) => Unauthorized("")
            case _ => InternalServerError()
          }
        }
        else
          Future(Unauthorized(""))
    }
  }

}
