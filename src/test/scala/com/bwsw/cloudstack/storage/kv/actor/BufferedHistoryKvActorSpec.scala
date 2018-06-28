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
import com.bwsw.cloudstack.storage.kv.entity
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.request.{GetHistoryRequest, ScrollHistoryRequest}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpecLike, Matchers}
import scaldi.{Injector, Module}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class BufferedHistoryKvActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with Matchers
  with Eventually
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
  private val history = KvHistory(storage.uUID, someKey, someValue, timestamp, entity.Set)
  private val historyListRetry = List(
    KvHistory(storage.uUID, someKey, null, timestamp, Delete),
    KvHistory(storage.uUID, null, null, timestamp, Clear))
  private val historyBulk = KvHistoryBulk(history :: historyListRetry)
  private val flushTimeout = 1000.millis

  private val someKeys = List.empty
  private val someOperations = List.empty
  private val someSort = List.empty
  private val someStart = 0
  private val someEnd = 0
  private val someSize = 50
  private val somePage = 1
  private val someScroll = 1000
  private val someScrollId = "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ=="
  private val historyList = List(
    History(someKey, someValue, timestamp, entity.Set),
    History(someKey, null, timestamp, Delete),
    History(null, null, timestamp, Clear))
  private val getHistoryRequest =
    GetHistoryRequest(
      storage.uUID,
      someKeys,
      someOperations,
      someStart,
      someEnd,
      someSort,
      someSize,
      somePage,
      someScroll)
  private val body = HistoryScrolledBody(3, 3, someScrollId, historyList)
  private val error = "ElasticsearchError"

  describe("a BufferedHistoryKvActor") {

    implicit val testModule: Injector = new Module {
      bind[HistoryProcessor] to historyProcessor
      bind[AppConfig] toNonLazy appConf
      bind[StorageCache] to storageCache
    }

    describe("(KvHistory)") {

      def test(flushSize: Int, verifyTimeoutFactor: Double) = {
        (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout)
        (appConf.getFlushHistorySize _).expects().returning(flushSize)
        val historyLogged = Promise[Boolean]
        expectHistoryProcessing(List(history), Future(None), historyLogged)
        val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
        bufferedHistoryKvActor ! history
        eventually(timeout(scaled(flushTimeout * verifyTimeoutFactor))) {
          historyLogged.isCompleted should be(true)
        }
      }

      def testRetry(flushSize: Int) = {
        (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout)
        (appConf.getFlushHistorySize _).expects().returning(flushSize).anyNumberOfTimes
        (appConf.getHistoryRetryLimit _).expects().returning(1)

        val historyAttempt = Promise[Boolean]
        expectHistoryProcessing(List(history), Future(Some(List(history))), historyAttempt)

        val historyRetry = Promise[Boolean]
        expectHistoryProcessing(List(history.makeAttempt), Future(None), historyRetry)

        val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
        bufferedHistoryKvActor ! history

        eventually(timeout(scaled(flushTimeout * 2.9))) {
          historyAttempt.isCompleted should be(true)
          historyRetry.isCompleted should be(true)
        }
      }

      it("should process just-in-time") {
        test(1, 0.9)
      }

      it("should process by timeout") {
        test(10, 1.9)
      }


      it("should process just-in-time with retry") {
        testRetry(1)
      }

      it("should process by timeout with retry") {
        testRetry(10)
      }
    }

    describe("KvHistoryBulk") {

      def test(flushSizeFactor: Int, verifyTimeoutFactor: Double) = {
        (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout)
        (appConf.getFlushHistorySize _).expects().returning(historyBulk.values.size * flushSizeFactor)
        val historyLogged = Promise[Boolean]
        expectHistoryProcessing(historyBulk.values.toList, Future(None), historyLogged)
        val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
        bufferedHistoryKvActor ! historyBulk
        eventually(timeout(scaled(flushTimeout * verifyTimeoutFactor))) {
          historyLogged.isCompleted should be(true)
        }
      }

      def testRetry(flushSizeFactor: Int) = {
        (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout)
        (appConf.getFlushHistorySize _).expects().returning(historyBulk.values.size * flushSizeFactor).anyNumberOfTimes

        val historyAttempt = Promise[Boolean]
        expectHistoryProcessing(historyBulk.values.toList, Future(Some(historyListRetry)), historyAttempt)
        (appConf.getHistoryRetryLimit _).expects().returning(1).repeat(historyListRetry.size)

        val historyRetry = Promise[Boolean]
        expectHistoryProcessing(historyListRetry.map(h => h.makeAttempt), Future(None), historyRetry)

        val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
        bufferedHistoryKvActor ! historyBulk

        eventually(timeout(scaled(flushTimeout * 2.9))) {
          historyAttempt.isCompleted should be(true)
          historyRetry.isCompleted should be(true)
        }
      }

      it("should process just-in-time") {
        test(1, 0.9)
      }

      it("should process by timeout") {
        test(2, 1.9)
      }

      it("should process just-in-time with retry") {
        testRetry(1)
      }

      it("should process with retry by timeout") {
        testRetry(2)
      }
    }

    describe("GetHistoryRequest") {
      (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout).once
      val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))

      def test(isHistoryEnabled: Option[Boolean], expect: Either[StorageError, HistoryResponseBody]) = {
        (storageCache.isHistoryEnabled _).expects(storage.uUID).returning(Future(isHistoryEnabled))
        if (expect.isRight)
          expectGetHistories()
        bufferedHistoryKvActor ! getHistoryRequest
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
      (appConf.getFlushHistoryTimeout _).expects().returning(flushTimeout).once
      val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))

      def test(scrollResult: Either[StorageError, HistoryScrolledBody]) = {
        expectScrollHistories().returning(Future(scrollResult))
        bufferedHistoryKvActor ! ScrollHistoryRequest(someScrollId, someScroll)
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
    (historyProcessor.scroll _).expects(someScrollId, someScroll)

  private def expectGetHistories() =
    (historyProcessor.get _)
      .expects(storage.uUID, someKeys, someOperations, someStart, someEnd, someSort, someSize, somePage, someScroll)
      .returning(Future(Right(body)))

  private def expectHistoryProcessing(
      expected: List[KvHistory],
      result: Future[Option[List[KvHistory]]],
      verifier: Promise[Boolean]): Unit = {
    (historyProcessor.save _).expects(expected).onCall { histories: List[KvHistory] =>
      verifier.success(true)
      result
    }
  }
}
