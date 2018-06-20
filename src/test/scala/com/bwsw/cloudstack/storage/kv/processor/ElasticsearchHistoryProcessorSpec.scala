package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.entity.History
import com.bwsw.cloudstack.storage.kv.message.response.GetHistoryResponse
import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, KvHistory, Set}
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.{indexInto, _}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkError, BulkResponse, BulkResponseItem, BulkResponseItems}
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortOrder}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHistoryProcessorSpec extends AsyncFunSpec with AsyncMockFactory {
  private val index = "history-storage-someStorage"
  private val `type` = "_doc"
  private val artificalKey = "cbIaz2MBpp4Ypizt4vT5"
  private val storageUuid = "someStorage"
  private val kvHistories = List(
    KvHistory("someStorage2", "someKey", "someValue", 1, Set),
    KvHistory("someStorage", "someKey1", "someValue", 1, Delete),
    KvHistory("someStorage2", "someKey1", "someValue2", 1, Clear)
  )
  private val historyList = List(
    History("key1", "value1", 1, Set),
    History("key1", null, 2, Delete),
    History(null, null, 3, Clear)
  )
  describe("An ElasticsearchHistoryProcessor") {
    val fakeClient = mock[HttpClient]
    val elasticsearchConfig = mock[ElasticsearchConfig]
    val processor = new ElasticsearchHistoryProcessor(fakeClient, elasticsearchConfig)

    describe("(save histories)") {
      it("should save a list of historical records") {
        val bulkResponse = BulkResponse(1, errors = false,
          kvHistories.map(history => BulkResponseItems(
            Some(BulkResponseItem(0, artificalKey, getHistoryIndex(history.storage), `type`, 1, forcedRefresh = false, found = false, created = true, "created", 200, None, None)),
            None,
            None,
            None)))
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, bulkResponse))))

        processor.save(kvHistories).map {
          case None => succeed
          case Some(_) => fail
        }
      }

      it("should save a part of the list of historical records and return those that failed") {
        val bulkResponseItems = List(
          BulkResponseItems(
            Some(BulkResponseItem(0, artificalKey, getHistoryIndex("someStorage2"), `type`, 1, forcedRefresh = false,
              found = false, created = true, "created", 200, None, None)),
            None,
            None,
            None),
          BulkResponseItems(
            Some(BulkResponseItem(1, artificalKey, getHistoryIndex("someStorage"), `type`, 1, forcedRefresh = false,
              found = false, created = true, "created", 500, Some(BulkError("", "", "", 1, getHistoryIndex("someStorage"))), None)),
            None,
            None,
            None),
          BulkResponseItems(
            Some(BulkResponseItem(2, artificalKey, getHistoryIndex("someStorage2"), `type`, 1, forcedRefresh = false,
              found = false, created = true, "created", 200, None, None)),
            None,
            None,
            None)
        )
        val bulkResponse = BulkResponse(1, errors = true, bulkResponseItems)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, bulkResponse))))

        processor.save(kvHistories).map {
          case None => fail
          case Some(List(history1)) =>
            if (kvHistories(1).equals(history1))
              succeed
            else
              fail
        }
      }

      it("should return all given kvHistories if request fails") {
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
        processor.save(kvHistories).map {
          case Some(list) =>
            if (kvHistories.diff(list).isEmpty)
              succeed
            else
              fail
          case None => fail
        }
      }
    }

    describe("(get kvHistories)") {
      it("should retrieve by storageUuid only") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid)).size(pageSize).scroll(keepAlive)
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(3, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("3", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(2).key, "value" -> historyList(2).value,
                "timestamp" -> historyList(2).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(2).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid).map {
          case Right(response) => assert(response == GetHistoryResponse(3, 3, page, scrollId, historyList))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid and keys") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val keys = List("key1")
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(termsQuery("key", keys))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, keys).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid and operations") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val operations = List(Set)
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(termsQuery("operation", operations.map(_.toString)))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(1, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, operations = operations).map {
          case Right(response) => assert(response == GetHistoryResponse(1, 1, page, scrollId, historyList.slice(0, 1)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid and timestamp start") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val start = 2
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(rangeQuery("timestamp").gte(start))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("3", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(2).key, "value" -> historyList(2).value,
                "timestamp" -> historyList(2).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(2).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, start = start).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(1, 3)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid and timestamp end") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val end = 2
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(rangeQuery("timestamp").lte(end))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, end = end).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid with different timestamp start and end") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val start = 1
        val end = 2
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(rangeQuery("timestamp").gte(start).lte(end))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, start = start, end = end).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid with same timestamp start and end") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val startAndEnd = 2
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(termQuery("timestamp", startAndEnd))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, start = startAndEnd, end = startAndEnd).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by storageUuid, keys, operations and timestamp start and end") {
        val pageSize = 50
        val keepAlive = "1 second"
        val scrollId = None
        val page = 1
        val keys = List("key1")
        val operations = List(Set)
        val start = 1
        val end = 2
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid))
          .size(pageSize).scroll(keepAlive).query(boolQuery().filter(List(
          termsQuery("key", keys), termsQuery("operation", operations.map(_.toString)), rangeQuery("timestamp").gte(start).lte(end))))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(2, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, sort = None,
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        (elasticsearchConfig.getScrollPageSize _).expects().returning(pageSize)
        (elasticsearchConfig.getScrollKeepAlive _).expects().returning(keepAlive)
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, keys, operations, start, end).map {
          case Right(response) => assert(response == GetHistoryResponse(2, 2, page, scrollId, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by all parameters") {
        val pageSize = 3
        val scrollId = Some("scrollId")
        val page = 2
        val keys = List("key1")
        val operations = List(Set)
        val start = 1
        val end = 2
        val scroll = "2 seconds"
        val sort = List("key", "-timestamp")
        val searchDefinition = ElasticDsl.search(ElasticsearchUtils.getStorageIndex(storageUuid)).
          size(pageSize).scroll(scroll).query(boolQuery().filter(List(
          termsQuery("key", keys), termsQuery("operation", operations.map(_.toString)), rangeQuery("timestamp").gte(start).lte(end)))).
          from(pageSize * (page - 1)).sortBy(List(FieldSortDefinition("key", order = SortOrder.ASC), FieldSortDefinition("timestamp", order = SortOrder.DESC)))
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
          SearchHits(8, 1, Array(
            SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, Some(Seq(historyList.head.key, historyList.head.timestamp.asInstanceOf[AnyRef])),
              Map("key" -> historyList.head.key, "value" -> historyList.head.value,
                "timestamp" -> historyList.head.timestamp.asInstanceOf[AnyRef], "operation" -> historyList.head.operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("2", storageUuid, `type`, 1, 1, None, None, None, None, None, Some(Seq(historyList(1).key, historyList(1).timestamp.asInstanceOf[AnyRef])),
              Map("key" -> historyList(1).key, "value" -> historyList(1).value,
                "timestamp" -> historyList(1).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(1).operation.toString),
              Map.empty, Map.empty, Map.empty),

            SearchHit("3", storageUuid, `type`, 1, 1, None, None, None, None, None, Some(Seq(historyList(2).key, historyList(2).timestamp.asInstanceOf[AnyRef])),
              Map("key" -> historyList(2).key, "value" -> historyList(2).value,
                "timestamp" -> historyList(2).timestamp.asInstanceOf[AnyRef], "operation" -> historyList(2).operation.toString),
              Map.empty, Map.empty, Map.empty)
          )))
        val sets = kvHistories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
          .expects(searchDefinition, SearchHttpExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, searchResponse))))

        processor.get(storageUuid, keys, operations, start, end, sort, page, pageSize, scroll).map {
          case Right(response) => assert(response == GetHistoryResponse(8, pageSize, page, scrollId, historyList))
          case _ => fail
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
