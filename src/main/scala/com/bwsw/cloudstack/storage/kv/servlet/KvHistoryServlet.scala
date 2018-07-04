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
import com.bwsw.cloudstack.storage.kv.entity.Operation
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import org.json4s.JsonAST._
import org.json4s.{CustomSerializer, DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.util.conversion.TypeConverter

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class KvHistoryServlet(
    system: ActorSystem,
    requestTimeout: FiniteDuration,
    historyRequestActor: ActorRef)
  extends ScalatraServlet
  with FutureSupport
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues + new OperationSerializer
  protected implicit val akkaTimeout: Timeout = requestTimeout
  protected implicit val itc: TypeConverter[String, Int] = new ToIntConverter
  protected implicit val ltc: TypeConverter[String, Long] = new ToLongConverter
  protected val fieldList = List("key", "value", "timestamp", "operation")

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = validate(params) match {
        case Some(getHistoryRequest) =>
          (historyRequestActor ? getHistoryRequest)
            .map {
              case Right(value) =>
                contentType = formats("json")
                value
              case Left(_: NotFoundError) => NotFound("")
              case Left(_: BadRequestError) => BadRequest("")
              case _ => InternalServerError()
            }
        case None => Future(BadRequest(""))
      }
    }
  }

  post("/") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json")) {
          parsedBody match {
            case JObject(List(("scroll", scrollId: JString), ("timeout", timeout: JInt))) =>
              val scrollRequest = KvHistoryScrollRequest(scrollId.values, timeout.values.toLong)
              (historyRequestActor ? scrollRequest).map {
                case Right(value) =>
                  contentType = formats("json")
                  value
                case Left(_: BadRequestError) => BadRequest("")
                case _ => InternalServerError("")
              }
            case _ => Future(BadRequest(""))
          }
        }
        else
          Future(BadRequest(""))
    }
  }


  protected def validate(params: Params): Option[KvHistoryGetRequest] = {
    try {
      Some(KvHistoryGetRequest(
        params("storage_uuid"),
        params.getOrElse("keys", "").split(",").filter(_.nonEmpty),
        params.getOrElse("operations", "").split(",").filter(_.nonEmpty).map(Operation.parse),
        params.getAs[Long]("start"),
        params.getAs[Long]("end"),
        params.getOrElse("sort", "").split(",").filter(fieldList.contains(_)),
        params.getAs[Int]("page"),
        params.getAs[Int]("size"),
        params.getAs[Long]("scroll")))
    } catch {
      case _: NumberFormatException => None
      case _: IllegalArgumentException => None
    }
  }

  private class ToIntConverter extends TypeConverter[String, Int] {
    override def apply(s: String) = Some(s.toInt)
  }

  private class ToLongConverter extends TypeConverter[String, Long] {
    override def apply(s: String) = Some(s.toLong)
  }

  private class OperationSerializer extends CustomSerializer[Operation](
    format => ( {
      case JString(s) => Operation.parse(s)
    }, {
      case op: Operation => JString(op.toString)
    }))

}
