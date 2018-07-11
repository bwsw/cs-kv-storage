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
import com.bwsw.cloudstack.storage.kv.entity.{Operation, SortField, Sorting}
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import org.json4s.JsonAST._
import org.json4s.{CustomSerializer, DefaultFormats, Formats, MappingException}
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
  protected implicit val toIntConverter: TypeConverter[String, Int] = new ToIntConverter
  protected implicit val toLongConverter: TypeConverter[String, Long] = new ToLongConverter

  protected val fieldList = List("key", "value", "timestamp", "operation")

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = parse(params) match {
        case Some(getHistoryRequest) =>
          (historyRequestActor ? getHistoryRequest)
            .map {
              case Right(value) =>
                contentType = formats("json")
                value
              case Left(_: NotFoundError) => NotFound("")
              case Left(_: BadRequestError) => BadRequest("")
              case _ => InternalServerError("")
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
            case json: JObject =>
              try {
                val scrollRequest = json.extract[KvHistoryScrollRequest]
                (historyRequestActor ? scrollRequest).map {
                  case Right(value) =>
                    contentType = formats("json")
                    value
                  case Left(_: BadRequestError) => BadRequest("")
                  case _ => InternalServerError("")
                }
              } catch {
                case e: MappingException => Future(BadRequest(""))
              }
            case _ => Future(BadRequest(""))
          }
        }
        else
          Future(BadRequest(""))
    }
  }


  protected def parse(params: Params): Option[KvHistoryGetRequest] = {
    try {
      val storage = params("storage_uuid")
      val keys = params.getOrElse("keys", "").split(",").filter(_.nonEmpty).toSet
      val operations = params.getOrElse("operations", "").split(",").filter(_.nonEmpty).map(Operation.parse).toSet
      val start = params.getAs[Long]("start")
      val end = params.getAs[Long]("end")
      val sort = params.getOrElse("sort", "").split(",").filter(_.nonEmpty).map { s =>
        if (s.startsWith(Sorting.DescPrefix))
          SortField(s.substring(Sorting.DescPrefix.length), Sorting.Desc)
        else SortField(s, Sorting.Asc)
      }.toSet
      val page = params.getAs[Int]("page")
      val size = params.getAs[Int]("size")
      val scroll = params.getAs[Long]("scroll")

      if (isValidStartEnd(start, end) && isValidSort(sort) && page.forall(_ > 0) && size.forall(_ > 0) && scroll
        .forall(_ > 0)) {
        val pageIfScroll = if (scroll.isEmpty) page else None
        Some(KvHistoryGetRequest(
          storage,
          keys,
          operations,
          start,
          end,
          sort,
          pageIfScroll,
          size,
          scroll))
      }
      else None
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

  private def isValidStartEnd(start: Option[Long], end: Option[Long]) = (start, end) match {
    case (None, None) => true
    case (Some(s), None) => s > 0
    case (None, Some(e)) => e > 0
    case (Some(s), Some(e)) => s > 0 && e > 0 && s <= e
  }

  private def isValidSort(sort: Set[SortField]) = {
    sort.map(_.field).forall(fieldList.contains(_)) && sort.groupBy(_.field).forall(_._2.size == 1)
  }

}
