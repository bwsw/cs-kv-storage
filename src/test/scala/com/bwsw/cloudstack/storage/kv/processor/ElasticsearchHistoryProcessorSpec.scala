package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, Set, KvHistory}
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.{indexInto, _}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem, BulkResponseItems}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHistoryProcessorSpec extends AsyncFunSpec with AsyncMockFactory {
  private val index = "history-storage-someStorage"
  private val `type` = "_doc"
  private val artificalKey = "cbIaz2MBpp4Ypizt4vT5"
  private val history = KvHistory("someStorage", "someKey", "someValue", 1, Set)
  private val histories = List(
    KvHistory("someStorage2", "someKey", "someValue", 1, Set),
    KvHistory("someStorage", "someKey1", "someValue", 1, Delete),
    KvHistory("someStorage2", "someKey1", "someValue2", 1, Clear)
  )

  describe("An ElasticsearchKvHistorian") {
    val fakeClient = mock[HttpClient]
    val processor = new ElasticsearchHistoryProcessor(fakeClient)

    describe("(save(histories))") {
      it("should save a list of historical records") {
        val bulkResponse = BulkResponse(1, errors = false,
          histories.map(history => BulkResponseItems(
            Some(BulkResponseItem(0, artificalKey, getHistoryIndex(history.storage), `type`, 1, forcedRefresh = false, found = false, created = true, "created", 200, None, None)),
            None,
            None,
            None)))
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, bulkResponse))))

        processor.save(histories.toVector).map {
          case Right(None) => succeed
          case Right(Some(_)) => fail
          case Left(_) => fail
        }
      }
      it("should return InternalError if request fails") {
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
        processor.save(histories.toVector).map {
          case Right(_) => fail
          case Left(_) => succeed
        }
      }
    }

  }

  private def getHistoryIndex(storage: String) = s"history-storage-$storage"

  private def getFields(history: KvHistory) = Map(
    "key" -> history.key,
    "value" -> history.value,
    "timestamp" -> history.timestamp,
    "operation" -> history.operation.toString)
}
