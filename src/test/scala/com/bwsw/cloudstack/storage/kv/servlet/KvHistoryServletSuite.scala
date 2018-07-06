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

  import scala.concurrent.ExecutionContext.Implicits.global

  private implicit val system: ActorSystem = ActorSystem()
  private val historyRequestActor = TestActorRef(new MockActor())

  private val storage = Storage("someStorage", "ACC", keepHistory = true)
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val timestamp = System.currentTimeMillis()
  private val total = 10
  private val pagesize = 3
  private val page = 2
  private val scroll = 1000
  private val scrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ=="
  private val historyList = List(
    History(someKey, someValue, timestamp, Set),
    History(someKey, null, timestamp, Delete),
    History(null, null, timestamp, Clear))
  private val pagedBody = PageSearchResult(total, pagesize, page, historyList)
  private val scrolledBody = ScrollSearchResult(total, pagesize, scrollId, historyList)
  private val scrollRequestBody: Array[Byte] = s"""{\"scroll\":\"$scrollId\",\"timeout\":$scroll}"""
  private val scrollRequestBodyWrong: Array[Byte] = s"""[{\"scroll\":\"$scrollId\"},{\"timeout\":$scroll}]"""
  private val jsonContentType = Seq(("Content-Type", "application/json"))
  private val textContentType = Seq(("Content-Type", "plain/text"))

  describe("a KvHistoryServlet") {
    addServlet(new KvHistoryServlet(system, 1.second, historyRequestActor), "/history/*")

    describe("(search)") {
      val pathBase = "/history/" + storage.uUID
      it("should return body with simple paging") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), page = Some(page)),
            () => Right(pagedBody)))
        get(pathBase, Seq(("page", page.toString), ("size", pagesize.toString))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getPagedJson)
        }
      }

      it("should return body with scroll") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll)),
            () => Right(scrolledBody)))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getScrolledJson)
        }
      }

      it("should return 400 Bad Request on request with bad operations") {
        get(pathBase, Seq(("operations", "set,clear,euthanasia"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad start") {
        get(pathBase, Seq(("start", "today"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with negative start") {
        get(pathBase, Seq(("start", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad end") {
        get(pathBase, Seq(("end", "today"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with negative end") {
        get(pathBase, Seq(("end", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with start > end") {
        get(pathBase, Seq(("start", "1000"), ("end", "500"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad size") {
        get(pathBase, Seq(("size", "large"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with negative size") {
        get(pathBase, Seq(("size", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad page") {
        get(pathBase, Seq(("page", "first"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with negative page") {
        get(pathBase, Seq(("page", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad scroll") {
        get(pathBase, Seq(("scroll", "1.day"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with negative scroll") {
        get(pathBase, Seq(("scroll", "-1000"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with bad sort") {
        get(pathBase, Seq(("sort", "bubble"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request on request with opposite sort") {
        get(pathBase, Seq(("sort", "timestamp,key,-timestamp"))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should successfully skip duplicating operations") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll), operations = immutable.Set(Set)),
            () => Right(scrolledBody)))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString), ("operations", "set,set"))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getScrolledJson)
        }
      }

      it("should successfully skip duplicating keys") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll), keys = immutable.Set("key1")),
            () => Right(scrolledBody)))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString), ("keys", "key1,key1"))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getScrolledJson)
        }
      }

      it("should successfully skip duplicating sorts") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll), sort = immutable.Set(SortField("key", Asc))),
            () => Right(scrolledBody)))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString), ("sort", "key,key"))) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getScrolledJson)
        }
      }

      it("should transfer BadRequestError from HistoryRequestActor") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll)),
            () => Left(BadRequestError())))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString))) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should transfer InternalError from HistoryRequestActor") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll)),
            () => Left(InternalError("Error"))))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString))) {
          status should equal(500)
          body should equal("")
        }
      }

      it("should transfer NotFoundError from HistoryRequestActor") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            getRequest(size = Some(pagesize), scroll = Some(scroll)),
            () => Left(NotFoundError())))
        get(pathBase, Seq(("size", pagesize.toString), ("scroll", scroll.toString))) {
          status should equal(404)
          body should equal("")
        }
      }
    }

    describe("(scroll)") {
      val pathBase = "/history/"
      it("should get next scroll") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(KvHistoryScrollRequest(scrollId, scroll), () => Right(scrolledBody)))
        post(pathBase, scrollRequestBody, jsonContentType) {
          status should equal(200)
          response.getContentType should include("application/json")
          body should equal(getScrolledJson)
        }
      }

      it("should return 400 Bad Request if Content Type is not application/json") {
        post(pathBase, scrollRequestBody, textContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request if body contains bad json") {
        post(pathBase, "{[}", jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should return 400 Bad Request if body contains wrong format") {
        post(pathBase, scrollRequestBodyWrong, jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should transfer BadRequestError from HistoryRequestActor") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvHistoryScrollRequest(scrollId, scroll),
            () => Left(BadRequestError())))
        post(pathBase, scrollRequestBody, jsonContentType) {
          status should equal(400)
          body should equal("")
        }
      }

      it("should transfer InternalError from HistoryRequestActor") {
        historyRequestActor.underlyingActor
          .clearAndExpect(ResponsiveExpectation(
            KvHistoryScrollRequest(scrollId, scroll),
            () => Left(InternalError("Error"))))
        post(pathBase, scrollRequestBody, jsonContentType) {
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
    storage.uUID,
    keys,
    operations,
    start,
    end,
    sort,
    page,
    size,
    scroll)

  private def getPagedJson: String =
    s"""{\"total\":$total,\"size\":$pagesize,\"page\":$page,\"items\":[""" +
      s"""{\"key\":\"$someKey\",\"value\":\"$someValue\",\"timestamp\":$timestamp,\"operation\":\"set\"},""" +
      s"""{\"key\":\"$someKey\",\"value\":null,\"timestamp\":$timestamp,\"operation\":\"delete\"},""" +
      s"""{\"key\":null,\"value\":null,\"timestamp\":$timestamp,\"operation\":\"clear\"}""" +
      "]}"

  private def getScrolledJson: String =
    s"""{\"total\":$total,\"size\":$pagesize,\"scrollId\":\"$scrollId\",\"items\":[""" +
      s"""{\"key\":\"$someKey\",\"value\":\"$someValue\",\"timestamp\":$timestamp,\"operation\":\"set\"},""" +
      s"""{\"key\":\"$someKey\",\"value\":null,\"timestamp\":$timestamp,\"operation\":\"delete\"},""" +
      s"""{\"key\":null,\"value\":null,\"timestamp\":$timestamp,\"operation\":\"clear\"}""" +
      "]}"
}
