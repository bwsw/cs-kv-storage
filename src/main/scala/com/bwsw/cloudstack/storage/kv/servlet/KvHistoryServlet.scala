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
import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, NotFoundError, InternalError}
import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, Set}
import com.bwsw.cloudstack.storage.kv.message.request.GetHistoryRequest
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class KvHistoryServlet(system: ActorSystem, requestTimeout: FiniteDuration, processor: HistoryProcessor, historyKvActor: ActorRef, elasticsearchConfig: ElasticsearchConfig)
  extends ScalatraServlet
    with FutureSupport
    with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues
  protected implicit val akkaTimeout: Timeout = requestTimeout

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = {
        try {
          val getHistoryRequest = GetHistoryRequest(params("storage_uuid"),
            multiParams.getOrElse("keys", List.empty),
            multiParams.getOrElse("operations", List.empty).map {
              case "set" => Set
              case "delete" => Delete
              case "clear" => Clear
              case _ => throw new OperationFormatException
            },
            params.getOrElse("start", "0").toLong,
            params.getOrElse("end", "0").toLong,
            multiParams.getOrElse("sort", List.empty),
            params.getOrElse("page", "1").toInt,
            params.getOrElse("size", elasticsearchConfig.getScrollPageSize.toString).toInt,
            params.getOrElse("scroll", ""))

          (historyKvActor ? getHistoryRequest)
            .map {
              case Right(value) =>
                contentType = formats("json")
                value
              case Left(_: NotFoundError) => NotFound("")
              case Left(_: BadRequestError) => BadRequest("")
              case Left(error: InternalError) => InternalServerError(error.message)
              case _ => InternalServerError()
            }
        } catch {
          case nfe: NumberFormatException => Future(BadRequest(""))
          case ofe: OperationFormatException => Future(BadRequest(""))
        }
      }
    }
  }

  private class OperationFormatException extends Exception

}
