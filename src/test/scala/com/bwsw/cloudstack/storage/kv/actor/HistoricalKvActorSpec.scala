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
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.entity.Operation.{Clear, Delete, Set}
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.error.{InternalError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.util.Clock
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike}
import scaldi.{Injector, Module}

import scala.concurrent.Future
import scala.concurrent.duration._

class HistoricalKvActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val historyTestProbe = TestProbe()
  private val kvProcessor = mock[KvProcessor]
  private val storageCache = mock[StorageCache]
  private val clock = mock[Clock]
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val storageUuid = "someStorage"
  private val keyValues = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val storageKeep = Storage("someStorage", "ACC", historyEnabled = true)
  private val storageDiscard = Storage("someStorage", "ACC", historyEnabled = false)
  private val timeout = 1000.millis
  private val timestamp = System.currentTimeMillis()
  private val exception = new RuntimeException("test exception")

  class HistoryKvActorMock(testProbe: TestProbe) extends HistoryKvActor {

    override def receive: Receive = {
      case msg: Any => testProbe.ref ! msg
    }
  }

  describe("a HistoricalKvActor") {

    implicit val testModule: Injector = new Module {
      bind[KvProcessor] to kvProcessor
      bind[StorageCache] to storageCache
      bind[HistoryKvActor] toProvider new HistoryKvActorMock(historyTestProbe)
      bind[Clock] to clock
    }
    val historicalKvActor = system.actorOf(Props(new HistoricalKvActor))

    describe("(KvGetRequest)") {

      def test(response: Any): Unit = {
        historicalKvActor ! KvGetRequest(storageUuid, someKey)
        expectMsg(response)
        expectHistoryMessage(None)
      }

      it("should process a request") {
        expectExistingStorage
        val answer = Right(someValue)
        (kvProcessor.get(_: String, _: String)).expects(storageUuid, someKey).returning(Future(answer))
        test(answer)
      }

      it("should return NotFoundError if the storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.get(_: String, _: String)).expects(storageUuid, someKey).never()
        test(Left(NotFoundError()))
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.get(_: String, _: String)).expects(storageUuid, someKey).never()
        test(Left(InternalError(exception.getMessage)))
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.get(_: String, _: String)).expects(storageUuid, someKey).returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)))
      }
    }

    describe("(KvMultiGetRequest)") {

      def test(response: Any): Unit = {
        historicalKvActor ! KvMultiGetRequest(storageUuid, keyValues.keys)
        expectMsg(response)
        expectHistoryMessage(None)
      }

      it("should process a request") {
        expectExistingStorage
        val answer = Right(keyValues.map(kv => kv._1 -> Some(kv._2)))
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys).returning(Future(answer))
        test(answer)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys).never()
        test(Left(NotFoundError()))
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys).never()
        test(Left(InternalError(exception.getMessage)))
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys)
          .returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)))
      }
    }

    describe("(KvSetRequest)") {

      def test(response: Any, history: Option[KvHistory]): Unit = {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        historicalKvActor ! KvSetRequest(storageUuid, someKey, someValue)
        expectMsg(response)
        expectHistoryMessage(history)
      }

      it("should process a request and log history") {
        expectHistoryEnabledStorage
        val answer = Right(())
        (kvProcessor.set(_: String, _: String, _: String)).expects(storageUuid, someKey, someValue)
          .returning(Future(answer))
        test(answer, Some(KvHistory(storageUuid, someKey, someValue, timestamp, Set)))
      }

      it("should process a request and skip history logging") {
        expectHistoryDisabledStorage
        val answer = Right(())
        (kvProcessor.set(_: String, _: String, _: String)).expects(storageUuid, someKey, someValue)
          .returning(Future(answer))
        test(answer, None)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.set(_: String, _: String, _: String)).expects(storageUuid, someKey, someValue).never()
        test(Left(NotFoundError()), None)
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.set(_: String, _: String, _: String)).expects(storageUuid, someKey, someValue).never()
        test(Left(InternalError(exception.getMessage)), None)
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.set(_: String, _: String, _: String)).expects(storageUuid, someKey, someValue)
          .returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)), None)
      }
    }

    describe("(KvMultiSetRequest)") {

      def test(response: Any, history: Option[KvHistoryBulk]): Unit = {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        historicalKvActor ! KvMultiSetRequest(storageUuid, keyValues)
        expectMsg(response)
        expectHistoryMessage(history)
      }

      it("should process a request and log history") {
        expectHistoryEnabledStorage
        val answer = Right(keyValues.map(kv => kv._1 -> true))
        (kvProcessor.set(_: String, _: Map[String, String])).expects(storageUuid, keyValues).returning(Future(answer))
        test(answer, Some(KvHistoryBulk(keyValues
          .map { case (key, value) => KvHistory(storageUuid, key, value, timestamp, Set) })))
      }

      it("should process a request and skip history logging") {
        expectHistoryDisabledStorage
        val answer = Right(keyValues.map(kv => kv._1 -> true))
        (kvProcessor.set(_: String, _: Map[String, String])).expects(storageUuid, keyValues).returning(Future(answer))
        test(answer, None)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.set(_: String, _: Map[String, String])).expects(storageUuid, keyValues).never()
        test(Left(NotFoundError()), None)
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.set(_: String, _: Map[String, String])).expects(storageUuid, keyValues).never()
        test(Left(InternalError(exception.getMessage)), None)
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.set(_: String, _: Map[String, String])).expects(storageUuid, keyValues)
          .returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)), None)
      }
    }

    describe("(KvDeleteRequest)") {

      def test(response: Any, history: Option[KvHistory]): Unit = {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        historicalKvActor ! KvDeleteRequest(storageUuid, someKey)
        expectMsg(response)
        expectHistoryMessage(history)
      }

      it("should process a request and log history") {
        expectHistoryEnabledStorage
        val answer = Right(())
        (kvProcessor.delete(_: String, _: String)).expects(storageUuid, someKey).returning(Future(answer))
        test(answer, Some(KvHistory(storageUuid, someKey, null, timestamp, Delete)))
      }

      it("should process a request and skip history logging") {
        expectHistoryDisabledStorage
        val answer = Right(())
        (kvProcessor.delete(_: String, _: String)).expects(storageUuid, someKey).returning(Future(answer))
        test(answer, None)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.delete(_: String, _: String)).expects(storageUuid, someKey).never()
        test(Left(NotFoundError()), None)
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.delete(_: String, _: String)).expects(storageUuid, someKey).never()
        test(Left(InternalError(exception.getMessage)), None)
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.delete(_: String, _: String)).expects(storageUuid, someKey).returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)), None)
      }
    }

    describe("(KvMultiDeleteRequest)") {

      def test(response: Any, history: Option[KvHistoryBulk]): Unit = {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        historicalKvActor ! KvMultiDeleteRequest(storageUuid, keyValues.keys)
        expectMsg(response)
        expectHistoryMessage(history)
      }

      it("should process a request and log history") {
        expectHistoryEnabledStorage
        val answer = Right(keyValues.map(kv => kv._1 -> true))
        (kvProcessor.delete(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys)
          .returning(Future(answer))
        test(answer, Some(KvHistoryBulk(keyValues
          .map { case (key, _) => KvHistory(storageUuid, key, null, timestamp, Delete) })))
      }

      it("should process a request and skip history logging") {
        expectHistoryDisabledStorage
        val answer = Right(keyValues.map(kv => kv._1 -> true))
        (kvProcessor.delete(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys)
          .returning(Future(answer))
        test(answer, None)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.delete(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys).never()
        test(Left(NotFoundError()), None)
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.delete(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys).never()
        test(Left(InternalError(exception.getMessage)), None)
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.delete(_: String, _: Iterable[String])).expects(storageUuid, keyValues.keys)
          .returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)), None)
      }
    }

    describe("(KvListRequest)") {

      def test(response: Any): Unit = {
        historicalKvActor ! KvListRequest(storageUuid)
        expectMsg(response)
        expectHistoryMessage(None)
      }

      it("should process a request") {
        expectExistingStorage
        val answer = Right(keyValues.keys.toList)
        (kvProcessor.list _).expects(storageUuid).returning(Future(answer))
        test(answer)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.list _).expects(storageUuid).never()
        test(Left(NotFoundError()))
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.list _).expects(storageUuid).never()
        test(Left(InternalError(exception.getMessage)))
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.list _).expects(storageUuid).returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)))
      }
    }

    describe("(KvClearRequest)") {

      def test(response: Any, history: Option[KvHistory]): Unit = {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        historicalKvActor ! KvClearRequest(storageUuid)
        expectMsg(response)
        expectHistoryMessage(history)
      }

      it("should process a request and log history") {
        expectHistoryEnabledStorage
        val answer = Right(())
        (kvProcessor.clear _).expects(storageUuid).returning(Future(answer))
        test(answer, Some(KvHistory(storageUuid, null, null, timestamp, Clear)))
      }

      it("should process a request and skip history logging") {
        expectHistoryDisabledStorage
        val answer = Right(())
        (kvProcessor.clear _).expects(storageUuid).returning(Future(answer))
        test(answer, None)
      }

      it("should return NotFoundError if storage does not exist") {
        expectNotFoundStorage
        (kvProcessor.clear _).expects(storageUuid).never()
        test(Left(NotFoundError()), None)
      }

      it("should return InternalError if the storage cache loading fails") {
        expectStorageCacheFailure
        (kvProcessor.clear _).expects(storageUuid).never()
        test(Left(InternalError(exception.getMessage)), None)
      }

      it("should return InternalError if the request processing fails") {
        expectExistingStorage
        (kvProcessor.clear _).expects(storageUuid).returning(Future.failed(exception))
        test(Left(InternalError(exception.getMessage)), None)
      }
    }
  }

  override def afterAll: Unit = {
    shutdown(system)
  }

  private def expectHistoryMessage(message: Option[Any]) = {
    if (message.nonEmpty)
      historyTestProbe.expectMsg(timeout, message.get)
    else
      historyTestProbe.expectNoMessage(timeout)
  }

  private def expectExistingStorage = {
    (storageCache.get _).expects(storageUuid).returning(Future(Some(storageKeep)))
  }

  private def expectNotFoundStorage = {
    (storageCache.get _).expects(storageUuid).returning(Future(None))
  }

  private def expectStorageCacheFailure = {
    (storageCache.get _).expects(storageUuid).returning(Future.failed(exception))
  }

  private def expectHistoryEnabledStorage = {
    (storageCache.get _).expects(storageUuid).returning(Future(Some(storageKeep)))
  }

  private def expectHistoryDisabledStorage = {
    (storageCache.get _).expects(storageUuid).returning(Future(Some(storageDiscard)))
  }

}
