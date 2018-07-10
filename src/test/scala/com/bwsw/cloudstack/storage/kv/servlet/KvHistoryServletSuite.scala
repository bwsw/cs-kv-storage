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
import com.bwsw.cloudstack.storage.kv.entity.Operation.{Clear, Delete, Set}
import com.bwsw.cloudstack.storage.kv.entity.Sorting.Asc
import com.bwsw.cloudstack.storage.kv.entity.{PageSearchResult, _}
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import com.bwsw.cloudstack.storage.kv.mock.MockActor
import com.bwsw.cloudstack.storage.kv.mock.MockActor.ResponsiveExpectation
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import org.scalatra.test.scalatest.ScalatraSuite

import scala.collection.immutable
import scala.concurrent.duration._

class KvHistoryServletSuite extends ScalatraSuite with FunSpecLike with MockFactory {

  private implicit val system: ActorSystem = ActorSystem()
  private val historyRequestActor = TestActorRef(new MockActor())

  private val storageUuid = "id"
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val timestamp = System.currentTimeMillis()
  private val page = 2
  private val scroll = 1000
  private val scrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ=="
  private val historyList = List(
    History(someKey, someValue, timestamp, Set),
    History(someKey, null, timestamp, Delete),
    History(null, null, timestamp, Clear))
  private val pageResult = PageSearchResult(15, 3, page, historyList)
  private val scrollResult = ScrollSearchResult(20, 5, scrollId, historyList)
  private val scrollRequestBody: Array[Byte] = s"""{\"scrollId\":\"$scrollId\",\"timeout\":$scroll}"""
  private val jsonContentType = Seq(("Content-Type", "application/json"))
  private val textContentType = Seq(("Content-Type", "plain/text"))

  describe("a KvHistoryServlet") {
    addServlet(new KvHistoryServlet(system, 1.second, historyRequestActor), "/history/*")

    describe("(search)") {

      val path = s"""/history/$storageUuid"""

      it("should return results for requests with page parameter") {
        historyRequestActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          getRequest(page = Some(page)),
          () => Right(pageResult)))
        get(path, Seq(("page", page.toString))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(pageResult))
        }
      }

      it("should return results for requests with scroll parameter") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(getRequest(scroll = Some(scroll)), () => Right(scrollResult)))
        get(path, Seq(("scroll", scroll.toString))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should return results for scroll request if both page and scroll parameters are specified") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(getRequest(scroll = Some(scroll)), () => Right(scrollResult)))
        get(path, Seq(("page", page.toString), ("scroll", scroll.toString))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should return 400 Bad Request for requests with invalid operations") {
        get(path, Seq(("operations", "set,clear,invalid"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid start") {
        get(path, Seq(("start", "today"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with negative start") {
        get(path, Seq(("start", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid end") {
        get(path, Seq(("end", "today"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with negative end") {
        get(path, Seq(("end", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with start > end") {
        get(path, Seq(("start", "1000"), ("end", "500"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid size") {
        get(path, Seq(("size", "large"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with negative size") {
        get(path, Seq(("size", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid page") {
        get(path, Seq(("page", "first"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with negative page") {
        get(path, Seq(("page", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid scroll") {
        get(path, Seq(("scroll", "1.day"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with negative scroll") {
        get(path, Seq(("scroll", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with invalid sort") {
        get(path, Seq(("sort", "bubble"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request for requests with opposite sort") {
        get(path, Seq(("sort", "timestamp,key,-timestamp"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should skip duplicating operations") {
        val operation = Set
        historyRequestActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          getRequest(
            scroll = Some(scroll),
            operations = immutable.Set(operation)), () => Right(scrollResult)))
        get(path, Seq(("scroll", scroll.toString), ("operations", s"""$operation,$operation"""))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should skip duplicating keys") {
        val key = "key1"
        historyRequestActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          getRequest(scroll = Some(scroll), keys = immutable.Set(key)),
          () => Right(scrollResult)))
        get(path, Seq(("scroll", scroll.toString), ("keys", s"""$key,$key"""))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should successfully skip duplicating sorts") {
        val key = "key"
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(scroll = Some(scroll), sort = immutable.Set(SortField(key, Asc))),
            () => Right(scrollResult)))
        get(path, Seq(("scroll", scroll.toString), ("sort", s"""$key,$key"""))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should return 400 Bad Request if HistoryRequestActor returns BadRequestError") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(getRequest(scroll = Some(scroll)), () => Left(BadRequestError())))
        get(path, Seq(("scroll", scroll.toString))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 500 Internal Error if HistoryRequestActor returns InternalError") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(scroll = Some(scroll)),
            () => Left(InternalError("Error"))))
        get(path, Seq(("scroll", scroll.toString))) {
          status should equal(500)
          body should equal("")
        }
      }

      it("should return 404 Not Found if HistoryRequestActor returns NotFoundError") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(getRequest(scroll = Some(scroll)), () => Left(NotFoundError())))
        get(path, Seq(("scroll", scroll.toString))) {
          status should equal(404)
          body should equal("")
        }
      }
    }

    describe("(scroll)") {

      val path = "/history/"

      it("should get results for a subsequent request") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvHistoryScrollRequest(scrollId, scroll), () => Right(scrollResult)))
        post(path, scrollRequestBody, jsonContentType) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getJson(scrollResult))
        }
      }

      it("should return 400 Bad Request if Content-Type is not application/json") {
        post(path, scrollRequestBody, textContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request if the body is invalid JSON") {
        post(path, "{[}", jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request if the body is invalid") {
        post(path, "{\"id\":\"value\"}", jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request if HistoryRequestActor returns BadRequestError") {
        historyRequestActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          KvHistoryScrollRequest(scrollId, scroll),
          () => Left(BadRequestError())))
        post(path, scrollRequestBody, jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 500 Internal Error if HistoryRequestActor returns InternalError") {
        historyRequestActor.underlyingActor.clearAndExpect(ResponsiveExpectation(
          KvHistoryScrollRequest(scrollId, scroll),
          () => Left(InternalError("Error"))))
        post(path, scrollRequestBody, jsonContentType) {
          status should equal(500)
          body should equal("")
        }
      }
    }
  }

  private def getRequest(
      keys: Set[String] = immutable.Set.empty,
      operations: Set[Operation] = immutable.Set.empty,
      start: Option[Long] = None,
      end: Option[Long] = None,
      sort: Set[SortField] = immutable.Set.empty,
      page: Option[Int] = None,
      size: Option[Int] = None,
      scroll: Option[Long] = None) = KvHistoryGetRequest(
    storageUuid,
    keys,
    operations,
    start,
    end,
    sort,
    page,
    size,
    scroll)

  private def getJson(result: PageSearchResult[History]): String =
    s"""{\"total\":${result.total},\"size\":${result.size},\"page\":${result.page},\"items\":[""" + result.items
      .map(h => getJson(h)).mkString(",") + "]}"

  private def getJson(result: ScrollSearchResult[History]): String =
    s"""{\"total\":${result.total},\"size\":${result.size},\"scrollId\":\"${result.scrollId}\",\"items\":[""" + result
      .items.map(h => getJson(h)).mkString(",") + "]}"

  private def getJson(history: History): String = {
    val key = if (history.key == null) "null" else s"""\"${history.key}\""""
    val value = if (history.value == null) "null" else s"""\"${history.value}\""""
    s"""{\"key\":$key,\"value\":$value,\"timestamp\":${history.timestamp},\"operation\":\"${history.operation}\"}"""
  }

}
