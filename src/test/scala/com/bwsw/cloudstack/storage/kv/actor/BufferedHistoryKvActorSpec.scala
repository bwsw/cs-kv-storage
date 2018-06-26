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
import akka.testkit.{ImplicitSender, TestKit, _}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, KvHistory, KvHistoryBulk, Set}
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike}
import scaldi.{Injector, Module}

import scala.concurrent.Future
import scala.concurrent.duration._

class BufferedHistoryKvActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
  with FunSpecLike
  with MockFactory
  with ImplicitSender
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  import scala.concurrent.ExecutionContext.Implicits.global

  private implicit val testModule: Injector = mocksModule
  private val historyProcessor = mock[HistoryProcessor]
  private val storageCache = mock[StorageCache]
  private val appConf = mock[AppConfig]
  private val storage = Storage("someStorage", "ACC", keepHistory = true)
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val timestamp = System.currentTimeMillis()
  private val operation = Delete
  private val history = KvHistory(storage.uUID, someKey, someValue, timestamp, operation)
  private val historyList = List(
    KvHistory(storage.uUID, someKey, someValue, timestamp, Set),
    KvHistory(storage.uUID, someKey, null, timestamp, Delete),
    KvHistory(storage.uUID, null, null, timestamp, Clear)
  )
  private val historyListRetry = List(
    KvHistory(storage.uUID, someKey, someValue, timestamp, Set, 1),
    KvHistory(storage.uUID, someKey, null, timestamp, Delete, 1),
    KvHistory(storage.uUID, null, null, timestamp, Clear, 1)
  )
  private val historyBulk = KvHistoryBulk(historyList)

  private def mocksModule = new Module {
    bind[HistoryProcessor] to historyProcessor
    bind[StorageCache] to storageCache
    bind[AppConfig] toNonLazy appConf
  }

  describe("a BufferedHistoryKvActor") {
    describe("KvHistory") {
      it("should process just-in-time") {
        within(400.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(800.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(1)
          (historyProcessor.save _).expects(List(history)).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! history
          expectNoMessage()
        }
      }

      it("should process by timeout") {
        within(200.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(100.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(10)
          (historyProcessor.save _).expects(List(history)).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! history
          expectNoMessage()
        }
      }

      it("should process just-in-time with retry") {
        within(100.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(200.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(1).anyNumberOfTimes
          (historyProcessor.save _).expects(List(history)).returning(Future(Some(List(history))))
          (appConf.getHistoryRetryLimit _).expects().returning(1)
          (historyProcessor.save _).expects(List(history.makeAttempt)).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! history
          expectNoMessage()
        }
      }

      it("should process with retry by timeout") {
        within(300.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(100.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(10).anyNumberOfTimes
          (historyProcessor.save _).expects(List(history)).returning(Future(Some(List(history))))
          (appConf.getHistoryRetryLimit _).expects().returning(1)
          (historyProcessor.save _).expects(List(history.makeAttempt)).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! history
          expectNoMessage()
        }
      }
    }

    describe("KvHistoryBulk") {
      it("should process just-in-time") {
        within(100.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(200.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(3)
          (historyProcessor.save _).expects(historyList).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! historyBulk
          expectNoMessage()
        }
      }

      it("should process by timeout") {
        within(200.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(100.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(10)
          (historyProcessor.save _).expects(historyList).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! historyBulk
          expectNoMessage()
        }
      }

      it("should process just-in-time with retry") {
        within(100.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(200.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(1).anyNumberOfTimes
          (historyProcessor.save _).expects(historyList).returning(Future(Some(historyList)))
          (appConf.getHistoryRetryLimit _).expects().returning(1).repeat(3).times
          (historyProcessor.save _).expects(historyListRetry).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! historyBulk
          expectNoMessage()
        }
      }

      it("should process with retry by timeout") {
        within(300.millis.dilated) {
          (appConf.getFlushHistoryTimeout _).expects().returning(100.millis.dilated)
          (appConf.getFlushHistorySize _).expects().returning(3).anyNumberOfTimes
          (historyProcessor.save _).expects(historyList).returning(Future(Some(List(
            KvHistory(storage.uUID, someKey, someValue, timestamp, Set),
            KvHistory(storage.uUID, someKey, null, timestamp, Delete)
          ))))
          (appConf.getHistoryRetryLimit _).expects().returning(1).repeat(2).times
          (historyProcessor.save _).expects(List(
            KvHistory(storage.uUID, someKey, someValue, timestamp, Set, 1),
            KvHistory(storage.uUID, someKey, null, timestamp, Delete, 1)
          )).returning(Future(None))
          val bufferedHistoryKvActor = system.actorOf(Props(new BufferedHistoryKvActor))
          bufferedHistoryKvActor ! historyBulk
          expectNoMessage()
        }
      }
    }
  }
}
