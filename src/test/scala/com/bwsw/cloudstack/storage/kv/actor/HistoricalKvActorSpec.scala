package com.bwsw.cloudstack.storage.kv.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.bwsw.cloudstack.storage.kv.cache.StorageCache
import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.error.NotFoundError
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.message.request._
import com.bwsw.cloudstack.storage.kv.mock.HistoryKvActorMock
import com.bwsw.cloudstack.storage.kv.mock.MockActor.SilentExpectation
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.util.Clock
import org.scalamock.handlers.CallHandler1
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike}
import scaldi.{Injector, Module}

import scala.concurrent.Future
import scala.concurrent.duration._


class HistoricalKvActorSpec
  extends TestKit(ActorSystem("cs-kv-storage"))
    with FunSpecLike
    with MockFactory
    with ImplicitSender
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  import scala.concurrent.ExecutionContext.Implicits.global

  private implicit val testModule: Injector = mocksModule
  private val historyKvActor = TestActorRef(new HistoryKvActorMock()).underlyingActor
  private val kvProcessor = mock[KvProcessor]
  private val storageCache = mock[StorageCache]
  private val clock = mock[Clock]
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val keyValues = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val storage = Storage("someStorage", "ACC", keepHistory = true)
  private val timeout = 200 millis
  private val timestamp = System.currentTimeMillis()

  private def mocksModule = new Module {
    bind[KvProcessor] to kvProcessor
    bind[StorageCache] to storageCache
    bind[HistoryKvActor] to historyKvActor
    bind[Clock] to clock
  }

  describe("a HistoricalKvActor") {

    val historicalKvActor = system.actorOf(Props(new HistoricalKvActor))
    describe("GetRequest") {
      it("should process a request") {
        val sender = TestProbe()
        storageExistingCheckPass
        val answer = Right(someValue)
        (kvProcessor.get(_: String, _: String)).expects(storage.uUID, someKey).returning(Future(answer))
        sender.send(historicalKvActor, KvGetRequest(storage.uUID, someKey))
        sender.expectMsg(answer)
        historyKvActor.check()
      }

      it("should return NotFoundError if storage does not exist") {
        val sender = TestProbe()
        storageExistingCheckFail
        (kvProcessor.get(_: String, _: String)).expects(storage.uUID, someKey).never()
        sender.send(historicalKvActor, KvGetRequest(storage.uUID, someKey))
        sender.expectMsg(Left(NotFoundError()))
        historyKvActor.check()
      }
    }

    describe("MultiGetRequest") {
      it("should process a request") {
        val sender = TestProbe()
        storageExistingCheckPass
        val answer = Right(keyValues.map(kv => kv._1 -> Some(kv._2)))
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storage.uUID, keyValues.keys).returning(Future(answer))
        sender.send(historicalKvActor, KvMultiGetRequest(storage.uUID, keyValues.keys))
        sender.expectMsg(answer)
        historyKvActor.check()
      }

      it("should return NotFoundError if storage does not exist") {
        val sender = TestProbe()
        storageExistingCheckFail
        (kvProcessor.get(_: String, _: Iterable[String])).expects(storage.uUID, keyValues.keys).never()
        sender.send(historicalKvActor, KvMultiGetRequest(storage.uUID, keyValues.keys))
        sender.expectMsg(Left(NotFoundError()))
        historyKvActor.check()
      }
    }

    describe("SetRequest") {
      it("should process a request and log history") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageHistoryEnabled
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(Some(KvHistory(storage.uUID, someKey, someValue, timestamp, Set))))
          (kvProcessor.set(_: String, _: String, _: String)).expects(storage.uUID, someKey, someValue).returning(Future(answer))
          sender.send(historicalKvActor, KvSetRequest(storage.uUID, someKey, someValue))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      def processWithoutLogging(reason: () => CallHandler1[String, Future[Option[Boolean]]]): Unit = {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          reason()
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(None))
          (kvProcessor.set(_: String, _: String, _: String)).expects(storage.uUID, someKey, someValue).returning(Future(answer))
          sender.send(historicalKvActor, KvSetRequest(storage.uUID, someKey, someValue))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should process a request but not log history") {
        processWithoutLogging(storageHistoryDisabled)
      }

      it("should process a request and log error if storage data inaccessible") {
        processWithoutLogging(storageHistoryCheckFailed)
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.set(_: String, _: String, _: String)).expects(storage.uUID, someKey, someValue).never()
          sender.send(historicalKvActor, KvSetRequest(storage.uUID, someKey, someValue))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }

    describe("MultiSetRequest") {
      it("should process a request") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageHistoryEnabled
          val answer = Right(keyValues.map(kv => kv._1 -> true))
          historyKvActor.expect(
            SilentExpectation(Some(KvHistoryBulk(keyValues.map {
              case (key, value) => KvHistory(storage.uUID, key, value, timestamp, Set)
            }))))
          (kvProcessor.set(_: String, _: Map[String, String])).expects(storage.uUID, keyValues).returning(Future(answer))
          sender.send(historicalKvActor, KvMultiSetRequest(storage.uUID, keyValues))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      def processWithoutLogging(reason: () => CallHandler1[String, Future[Option[Boolean]]]): Unit = {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          reason()
          val answer = Right(keyValues.map(kv => kv._1 -> true))
          historyKvActor.expect(SilentExpectation(None))
          (kvProcessor.set(_: String, _: Map[String, String])).expects(storage.uUID, keyValues).returning(Future(answer))
          sender.send(historicalKvActor, KvMultiSetRequest(storage.uUID, keyValues))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should process a request but not log history") {
        processWithoutLogging(storageHistoryDisabled)
      }

      it("should process a request and log error if storage data inaccessible") {
        processWithoutLogging(storageHistoryCheckFailed)
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.set(_: String, _: Map[String, String])).expects(storage.uUID, keyValues).never()
          sender.send(historicalKvActor, KvMultiSetRequest(storage.uUID, keyValues))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }

    describe("DeleteRequest") {
      it("should process a request and log history") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageHistoryEnabled
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(Some(KvHistory(storage.uUID, someKey, null, timestamp, Delete))))
          (kvProcessor.delete(_: String, _: String)).expects(storage.uUID, someKey).returning(Future(answer))
          sender.send(historicalKvActor, KvDeleteRequest(storage.uUID, someKey))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      def processWithoutLogging(reason: () => CallHandler1[String, Future[Option[Boolean]]]): Unit = {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          reason()
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(None))
          (kvProcessor.delete(_: String, _: String)).expects(storage.uUID, someKey).returning(Future(answer))
          sender.send(historicalKvActor, KvDeleteRequest(storage.uUID, someKey))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should process a request but not log history") {
        processWithoutLogging(storageHistoryDisabled)
      }

      it("should process a request and log error if storage data inaccessible") {
        processWithoutLogging(storageHistoryCheckFailed)
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.delete(_: String, _: String)).expects(storage.uUID, someKey).never()
          sender.send(historicalKvActor, KvDeleteRequest(storage.uUID, someKey))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }

    describe("MultiDeleteRequest") {
      it("should process a request and log history") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageHistoryEnabled
          val answer = Right(keyValues.map(kv => kv._1 -> true))
          historyKvActor.expect(
            SilentExpectation(Some(KvHistoryBulk(keyValues.map {
              case (key, _) => KvHistory(storage.uUID, key, null, timestamp, Delete)
            }))))
          (kvProcessor.delete(_: String, _: Iterable[String])).expects(storage.uUID, keyValues.keys).returning(Future(answer))
          sender.send(historicalKvActor, KvMultiDeleteRequest(storage.uUID, keyValues.keys))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      def processWithoutLogging(reason: () => CallHandler1[String, Future[Option[Boolean]]]): Unit = {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          reason()
          val answer = Right(keyValues.map(kv => kv._1 -> true))
          historyKvActor.expect(SilentExpectation(None))
          (kvProcessor.delete(_: String, _: Iterable[String])).expects(storage.uUID, keyValues.keys).returning(Future(answer))
          sender.send(historicalKvActor, KvMultiDeleteRequest(storage.uUID, keyValues.keys))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should process a request but not log history") {
        processWithoutLogging(storageHistoryDisabled)
      }

      it("should process a request and log error if storage data inaccessible") {
        processWithoutLogging(storageHistoryCheckFailed)
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.delete(_: String, _: String)).expects(storage.uUID, someKey).never()
          sender.send(historicalKvActor, KvMultiDeleteRequest(storage.uUID, keyValues.keys))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }

    describe("ListRequest") {
      it("should process a request") {
        within(timeout) {
          val sender = TestProbe()
          storageExistingCheckPass
          val answer = Right(keyValues.keys.toList)
          (kvProcessor.list _).expects(storage.uUID).returning(Future(answer))
          sender.send(historicalKvActor, KvListRequest(storage.uUID))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.list _).expects(storage.uUID).never()
          sender.send(historicalKvActor, KvListRequest(storage.uUID))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }

    describe("ClearRequest") {
      it("should process a request and log history") {
        (clock.currentTimeMillis _).expects().returning(timestamp)
        within(timeout) {
          val sender = TestProbe()
          storageHistoryEnabled
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(Some(KvHistory(storage.uUID, null, null, timestamp, Clear))))
          (kvProcessor.clear _).expects(storage.uUID).returning(Future(answer))
          sender.send(historicalKvActor, KvClearRequest(storage.uUID))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      def processWithoutLogging(reason: () => CallHandler1[String, Future[Option[Boolean]]]): Unit = {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          reason()
          val answer = Right(())
          historyKvActor.expect(SilentExpectation(None))
          (kvProcessor.clear _).expects(storage.uUID).returning(Future(answer))
          sender.send(historicalKvActor, KvClearRequest(storage.uUID))
          sender.expectMsg(answer)
          expectNoMessage
          historyKvActor.check()
        }
      }

      it("should process a request but not log history") {
        processWithoutLogging(storageHistoryDisabled)
      }

      it("should process a request and log error if storage data inaccessible") {
        processWithoutLogging(storageHistoryCheckFailed)
      }

      it("should return NotFoundError if storage does not exist") {
        within(timeout) {
          (clock.currentTimeMillis _).expects().returning(timestamp)
          val sender = TestProbe()
          storageExistingCheckFail
          (kvProcessor.clear _).expects(storage.uUID).never()
          sender.send(historicalKvActor, KvClearRequest(storage.uUID))
          sender.expectMsg(Left(NotFoundError()))
          expectNoMessage
          historyKvActor.check()
        }
      }
    }
  }

  private def storageExistingCheckPass = {
    (storageCache.get _).expects(storage.uUID).returning(Future(Some(storage)))
  }

  private def storageExistingCheckFail = {
    (storageCache.get _).expects(storage.uUID).returning(Future(None))
  }

  private def storageHistoryEnabled = {
    storageExistingCheckPass
    (storageCache.isHistoryEnabled _).expects(storage.uUID).returning(Future(Some(true)))
  }

  def storageHistoryDisabled(): CallHandler1[String, Future[Option[Boolean]]] = {
    storageExistingCheckPass
    (storageCache.isHistoryEnabled _).expects(storage.uUID).returning(Future(Some(false)))
  }

  def storageHistoryCheckFailed(): CallHandler1[String, Future[Option[Boolean]]] = {
    storageExistingCheckPass
    (storageCache.isHistoryEnabled _).expects(storage.uUID).returning(Future(None))
  }

  override def beforeEach: Unit = {
    historyKvActor.clear()
  }

  override def afterAll: Unit = {
    shutdown(system)
  }
}
