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
import akka.testkit.{ImplicitSender, TestKit}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpecLike
import scaldi.{Injector, Module}

import scala.collection.immutable
import scala.concurrent.Future

class DefaultHistoryRequestActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val historyProcessor = mock[HistoryProcessor]
  private val appConf = mock[AppConfig]
  private val storageCache = mock[StorageCache]

  private val storage = Storage("someStorage", "ACC", keepHistory = true)
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val timestamp = System.currentTimeMillis()
  private val someKeys = immutable.Set.empty[String]
  private val someOperations = immutable.Set.empty[Operation]
  private val someSort = immutable.Set.empty[SortField]
  private val someStart = Some(0.asInstanceOf[Long])
  private val someEnd = Some(0.asInstanceOf[Long])
  private val someSize = Some(50)
  private val somePage = Some(1)
  private val someScroll = Some(1000.asInstanceOf[Long])
  private val someScrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ=="
  private val historyList = List(
    History(someKey, someValue, timestamp, Set),
    History(someKey, null, timestamp, Delete),
    History(null, null, timestamp, Clear))
  private val getHistoryRequest =
    KvHistoryGetRequest(
      storage.uUID,
      someKeys,
      someOperations,
      someStart,
      someEnd,
      someSort,
      someSize,
      somePage,
      someScroll)
  private val body = ScrollSearchResult(3, 3, someScrollId, historyList)
  private val error = "ElasticsearchError"

  describe("a PagedHistoryRequestActor") {
    implicit val testModule: Injector = new Module {
      bind[HistoryProcessor] to historyProcessor
      bind[AppConfig] toNonLazy appConf
      bind[StorageCache] to storageCache
    }

    describe("GetHistoryRequest") {
      val pagedHistoryRequestActor = system.actorOf(Props(new DefaultHistoryRequestActor()))

      def test(isHistoryEnabled: Option[Boolean], expect: Either[StorageError, SearchResult[History]]) = {
        (storageCache.isHistoryEnabled _).expects(storage.uUID).returning(Future(isHistoryEnabled))
        if (expect.isRight)
          expectGetHistories()
        pagedHistoryRequestActor ! getHistoryRequest
        expectMsg(expect)
      }

      it("should transfer response body from processor if storage exists and support history") {
        test(Some(true), Right(body))
      }

      it("should return BadRequestError if storage does not support history") {
        test(Some(false), Left(BadRequestError()))
      }

      it("should return NotFoundError if storage does not exist") {
        test(None, Left(NotFoundError()))
      }
    }

    describe("ScrollHistoryRequest") {
      val pagedHistoryRequestActor = system.actorOf(Props(new DefaultHistoryRequestActor))

      def test(scrollResult: Either[StorageError, ScrollSearchResult[History]]) = {
        expectScrollHistories().returning(Future(scrollResult))
        pagedHistoryRequestActor ! KvHistoryScrollRequest(someScrollId, someScroll.get)
        expectMsg(scrollResult)
      }

      it("should be able to transfer next scroll from processor") {
        test(Right(body))
      }

      it("should be able to transfer BadRequestError from processor") {
        test(Left(BadRequestError()))
      }

      it("should be able to transfer InternalError from processor") {
        test(Left(InternalError(error)))
      }
    }
  }

  private def expectScrollHistories() =
    (historyProcessor.scroll _).expects(someScrollId, someScroll.get)

  private def expectGetHistories() =
    (historyProcessor.get _)
      .expects(storage.uUID, someKeys, someOperations, someStart, someEnd, someSort, someSize, somePage, someScroll)
      .returning(Future(Right(body)))
}
