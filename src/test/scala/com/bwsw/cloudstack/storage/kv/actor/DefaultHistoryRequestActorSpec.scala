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
import com.bwsw.cloudstack.storage.kv.entity.Operation.Set
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError,
  UnauthorizedError}
import com.bwsw.cloudstack.storage.kv.message.request.{KvHistoryGetRequest, KvHistoryScrollRequest}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import com.bwsw.cloudstack.storage.kv.util.elasticsearch.StorageType
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
  private val storageCache = mock[StorageCache]

  private val scrollTimeout = Some(1000.asInstanceOf[Long])
  private val scrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ=="
  private val storage = Storage("id", StorageType.Account, historyEnabled = true, "secret")
  private val getHistoryRequest =
    KvHistoryGetRequest(
      storage.uuid,
      storage.secretKey,
      immutable.Set.empty[String],
      immutable.Set.empty[Operation],
      Some(System.currentTimeMillis()),
      Some(System.currentTimeMillis()),
      immutable.Set.empty[SortField],
      Some(50),
      Some(1),
      scrollTimeout)
  private val historyList = List(History("key", "value", System.currentTimeMillis(), Set))
  private val scrollResult = ScrollSearchResult(3, 3, scrollId, historyList)
  private val error = InternalError("ElasticsearchError")

  describe("a DefaultHistoryRequestActor") {

    implicit val testModule: Injector = new Module {
      bind[HistoryProcessor] to historyProcessor
      bind[StorageCache] to storageCache
    }
    val defaultHistoryRequestActor = system.actorOf(Props(new DefaultHistoryRequestActor()))

    describe("(KvHistoryGetRequest)") {

      def test(storage: Option[Storage], expect: Either[StorageError, SearchResult[History]]) = {
        (storageCache.get _).expects(getHistoryRequest.storageUuid).returning(Future(storage))
        if (storage.exists(storage => storage.historyEnabled && storage.storageType != StorageType.Temporary))
          (historyProcessor.get _).expects(
            getHistoryRequest.storageUuid,
            getHistoryRequest.keys,
            getHistoryRequest.operations,
            getHistoryRequest.start,
            getHistoryRequest.end,
            getHistoryRequest.sort,
            getHistoryRequest.page,
            getHistoryRequest.size,
            getHistoryRequest.scroll).returning(Future(expect))
        defaultHistoryRequestActor ! getHistoryRequest
        expectMsg(expect)
      }

      it("should process a request if the storage exists and supports history") {
        test(Some(storage), Right(scrollResult))
      }

      it("should return BadRequestError if the storage does not support history") {
        test(Some(storage.copy(historyEnabled = false)), Left(BadRequestError()))
      }

      it("should return NotFoundError if the storage does not exist") {
        test(None, Left(NotFoundError()))
      }

      it("should return UnauthorizedError if given secret key is invalid") {
        (storageCache.get _).expects(getHistoryRequest.storageUuid)
          .returning(Future(Some(storage.copy(secretKey = "anotherSecret"))))
        defaultHistoryRequestActor ! getHistoryRequest
        expectMsg(Left(UnauthorizedError()))
      }

      it("should return InternalError if the processor returns InternalError") {
        test(Some(storage), Left(error))
      }
    }

    describe("(KvHistoryScrollRequest)") {

      def test(scrollResult: Either[StorageError, ScrollSearchResult[History]]) = {
        (historyProcessor.scroll _).expects(scrollId, scrollTimeout.get).returning(Future(scrollResult))
        defaultHistoryRequestActor ! KvHistoryScrollRequest(scrollId, scrollTimeout.get)
        expectMsg(scrollResult)
      }

      it("should process a request") {
        test(Right(scrollResult))
      }

      it("should return BadRequestError if the processor returns BadRequestError") {
        test(Left(BadRequestError()))
      }

      it("should return InternalError if the processor returns InternalError") {
        test(Left(error))
      }
    }
  }
}
