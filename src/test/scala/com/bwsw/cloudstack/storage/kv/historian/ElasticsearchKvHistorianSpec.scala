package com.bwsw.cloudstack.storage.kv.historian

import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.bwsw.cloudstack.storage.kv.processor.ElasticsearchHistoryProcessor
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.indexInto
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.indexes.IndexDefinition
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem, BulkResponseItems}

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchKvHistorianSpec extends AsyncFunSpec with AsyncMockFactory {
  private val index = "storage-someStorage-history"
  private val `type` = "_doc"
  private val artificalKey = "cbIaz2MBpp4Ypizt4vT5"
  private val history = KvHistory("someStorage", "someKey", "someValue", 1, "someOperation")
  private val histories = List(
    KvHistory("someStorage2", "someKey", "someValue", 1, "someOperation"),
    KvHistory("someStorage", "someKey1", "someValue", 1, "someOperation"),
    KvHistory("someStorage2", "someKey1", "someValue2", 1, "someOperation")
  )

  describe("An ElasticsearchKvHistorian") {
    val fakeClient = mock[HttpClient]
    val historian = new ElasticsearchHistoryProcessor(fakeClient)

    describe("(save(history))") {
      it("should save single historical record") {
        val indexResponse = IndexResponse(artificalKey, index, `type`, 1, "created", forcedRefresh = false, null)
        (fakeClient.execute[IndexDefinition, IndexResponse](_: IndexDefinition)(_: HttpExecutable[IndexDefinition, IndexResponse], _: ExecutionContext))
          .expects(indexInto(index / `type`) fields getFields(history), IndexHttpExecutable, *).returning(Future(Right(RequestSuccess(201, Option.empty, Map.empty, indexResponse))))

        historian.save(history).map {
          case Right(_) => succeed
          case Left(_) => fail
        }
      }
      it("should return InternalError if request fails") {
        val indexResponse = IndexResponse(artificalKey, index, `type`, 1, "created", forcedRefresh = false, null)
        (fakeClient.execute[IndexDefinition, IndexResponse](_: IndexDefinition)(_: HttpExecutable[IndexDefinition, IndexResponse], _: ExecutionContext))
          .expects(indexInto(index / `type`) fields getFields(history), IndexHttpExecutable, *)
          .returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))

        historian.save(history).map {
          case Right(_) => fail
          case Left(_) => succeed
        }
      }
    }
    describe("(save(histories))") {
      it("should save a list of historical records") {
        val bulkResponse = BulkResponse(1, errors = false,
          histories.map(history => BulkResponseItems(
            Some(BulkResponseItem(0, artificalKey, getHistoryIndex(history.storage), `type`, 1, forcedRefresh = false, found = false, created = false, "created", 200, None, None)),
            None,
            None,
            None)))
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, bulkResponse))))

        historian.save(histories.toVector).map {
          case Right(_) => succeed
          case Left(_) => fail
        }
      }
      it("should return InternalError if request fails") {
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
        historian.save(histories.toVector).map {
          case Right(_) => fail
          case Left(_) => succeed
        }
      }
    }

  }

  private def getHistoryIndex(storage: String) = s"storage-$storage-history"

  private def getFields(history: KvHistory) = Map(
    "key" -> history.key,
    "value" -> history.value,
    "timestamp" -> history.timestamp,
    "operation" -> history.operation)
}
