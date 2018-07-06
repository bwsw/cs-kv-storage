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

package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError}
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.{indexInto, _}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkError, BulkResponse, BulkResponseItem, BulkResponseItems}
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.searches.{SearchDefinition, SearchScrollDefinition}
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortOrder}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHistoryProcessorSpec extends AsyncFunSpec with AsyncMockFactory {
  private val defaultKeys = immutable.Set.empty[String]
  private val defaultOperations = immutable.Set.empty[Operation]
  private val defaultSort = immutable.Set.empty[SortField]
  private val defaultStart = None
  private val defaultEnd = None
  private val defaultSize = None
  private val defaultSizeValue = 50
  private val defaultPage = None
  private val defaultPageValue = 1
  private val defaultScroll = None
  private val someScrollId = Some("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ==")
  private val someTimeout = 1000
  private val defaultScrollId = None
  private val `type` = "_doc"
  private val artificialKey = "cbIaz2MBpp4Ypizt4vT5"
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
    implicit val client: HttpClient = fakeClient
    implicit val elasticsearchConfig: ElasticsearchConfig = mock[ElasticsearchConfig]
    val processor = new ElasticsearchHistoryProcessor(fakeClient, elasticsearchConfig)

    describe("(save histories)") {
      it("should save a list of historical records") {
        val bulkResponse = BulkResponse(1, errors = false,
          kvHistories.map(history => BulkResponseItems(
            Some(BulkResponseItem(
              0,
              artificialKey,
              getHistoryIndex(history.storage),
              `type`,
              1,
              forcedRefresh = false,
              found = false,
              created = true,
              "created",
              200,
              None,
              None)),
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
            Some(BulkResponseItem(
              0, artificialKey, getHistoryIndex("someStorage2"), `type`, 1, forcedRefresh = false,
              found = false, created = true, "created", 200, None, None)),
            None,
            None,
            None),
          BulkResponseItems(
            Some(BulkResponseItem(
              1,
              artificialKey,
              getHistoryIndex("someStorage"),
              `type`,
              1,
              forcedRefresh = false,
              found = false, created = true, "created", 500, Some(BulkError("", "", "", 1, getHistoryIndex("someStorage"))), None)),
            None,
            None,
            None),
          BulkResponseItems(
            Some(BulkResponseItem(
              2, artificialKey, getHistoryIndex("someStorage2"), `type`, 1, forcedRefresh = false,
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

    describe("(get histories)") {
      it("should retrieve by storageUuid only") {
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).size(defaultSizeValue)
        val searchResponse = getSearchResponse(historyList, 3, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(3, 3, defaultPageValue, historyList))
          case _ => fail
        }
      }

      it("should retrieve by keys") {
        val keys = immutable.Set("key1")
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(termsQuery("key", keys))
        val searchResponse = getSearchResponse(historyList.slice(0, 2), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, keys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by operations") {
        val operations: immutable.Set[Operation] = immutable.Set(Set)
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(termsQuery("operation", operations.map(_.toString)))
        val searchResponse = getSearchResponse(historyList.slice(0, 1), 1, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, operations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(1, 1, defaultPageValue, historyList.slice(0, 1)))
          case _ => fail
        }
      }

      it("should retrieve by timestamp start") {
        val start = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(rangeQuery("timestamp").gte(start.get))
        val searchResponse = getSearchResponse(historyList.slice(1, 3), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, start, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(1, 3)))
          case _ => fail
        }
      }

      it("should retrieve by timestamp end") {
        val end = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(rangeQuery("timestamp").lte(end.get))
        val searchResponse = getSearchResponse(historyList.slice(0, 2), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, end, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by different timestamp start and end") {
        val start = Some(1.asInstanceOf[Long])
        val end = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(rangeQuery("timestamp").gte(start.get).lte(end.get))
        val searchResponse = getSearchResponse(historyList.slice(0, 2), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, start, end, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by same timestamp start and end") {
        val startAndEnd = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(termQuery("timestamp", startAndEnd.get))
        val searchResponse = getSearchResponse(historyList.slice(0, 2), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, startAndEnd, startAndEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by keys, operations and timestamp start and end") {
        val keys = immutable.Set("key1")
        val operations: immutable.Set[Operation] = immutable.Set(Set)
        val start = Some(1.asInstanceOf[Long])
        val end = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid))
          .size(defaultSizeValue).query(boolQuery().filter(List(
          termsQuery("key", keys),
          termsQuery("operation", operations.map(_.toString)),
          rangeQuery("timestamp").gte(start.get).lte(end.get))))
        val searchResponse = getSearchResponse(historyList.slice(0, 2), 2, defaultScrollId)
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, keys, operations, start, end, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Right(response) => assert(response == PageSearchResult(2, 2, defaultPageValue, historyList.slice(0, 2)))
          case _ => fail
        }
      }

      it("should retrieve by all parameters") {
        val pageSize = Some(3)
        val scrollId = Some("scrollId")
        val page = Some(2)
        val keys = immutable.Set("key1")
        val operations: immutable.Set[Operation] = immutable.Set(Set)
        val start = Some(1.asInstanceOf[Long])
        val end = Some(2.asInstanceOf[Long])
        val scroll = Some(2000.asInstanceOf[Long])
        val sort = immutable.Set(SortField("key", Sorting.Asc), SortField("timestamp", Sorting.Desc))
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).
          size(pageSize.get).scroll(scroll.get + "ms").query(boolQuery().filter(List(
          termsQuery("key", keys),
          termsQuery("operation", operations.map(_.toString)),
          rangeQuery("timestamp").gte(start.get).lte(end.get)))).
          from(pageSize.get * (page.get - 1)).sortBy(List(
          FieldSortDefinition("key", order = SortOrder.ASC),
          FieldSortDefinition("timestamp", order = SortOrder.DESC)))
        val searchResponse = getSearchResponse(historyList, 8, scrollId, Seq("key", "timestamp"))

        expectSearch(searchDefinition, isSizeSet = true).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, keys, operations, start, end, sort, page, pageSize, scroll).map {
          case Right(response) => assert(response == ScrollSearchResult(
            8,
            pageSize.get.toLong,
            scrollId.get,
            historyList))
          case _ => fail
        }
      }

      it("should return InternalError if Elasticsearch request fails") {
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).size(defaultSizeValue)
        expectSearch(searchDefinition).returning(getRequestFailureFuture())

        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if bad value of String field returned form Elasticsearch") {
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).size(defaultSizeValue)
        val searchResponse = getBadResponse(Map("key" -> "someKey", "value" -> 1.asInstanceOf[AnyRef], "timestamp" -> 1.asInstanceOf[AnyRef], "operation" -> "set"))
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))


        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if bad value of Long field returned form Elasticsearch") {
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).size(defaultSizeValue)
        val searchResponse = getBadResponse(Map("key" -> "someKey", "value" -> "someValue", "timestamp" -> "1", "operation" -> "set"))
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if bad value of Operation returned form Elasticsearch") {
        val searchDefinition = ElasticDsl.search(getHistoryIndex(storageUuid)).size(defaultSizeValue)
        val searchResponse = getBadResponse(Map("key" -> "someKey", "value" -> "someValue", "timestamp" -> 1.asInstanceOf[AnyRef], "operation" -> "CL33Я"))
        expectSearch(searchDefinition).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, defaultKeys, defaultOperations, defaultStart, defaultEnd, defaultSort, defaultPage, defaultSize, defaultScroll).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(scroll by request)") {
      it("should return next page of scroll") {
        val total = 5
        val scrollDefinition = ElasticDsl.searchScroll(someScrollId.get, someTimeout + "ms")
        val searchScrollResponse = getSearchResponse(historyList, total, someScrollId, Seq.empty)
        expectScroll(scrollDefinition).returning(getRequestSuccessFuture(searchScrollResponse))

        processor.scroll(someScrollId.get, someTimeout).map {
          case Right(body) => assert(body == ScrollSearchResult(total, historyList.size, someScrollId.get, historyList))
          case _ => fail
        }
      }

      it("should return BadRequestError if scroll has expired or doesn't exists") {
        val scrollDefinition = ElasticDsl.searchScroll(someScrollId.get, someTimeout + "ms")
        expectScroll(scrollDefinition).returning(getRequestFailureFuture(404))

        processor.scroll(someScrollId.get, someTimeout).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return BadRequestError if scrollId is invalid") {
        val scrollDefinition = ElasticDsl.searchScroll(someScrollId.get, someTimeout + "ms")
        expectScroll(scrollDefinition).returning(getRequestFailureFuture(400))

        processor.scroll(someScrollId.get, someTimeout).map {
          case Left(_: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if Elasticsearch request failed") {
        val scrollDefinition = ElasticDsl.searchScroll(someScrollId.get, someTimeout + "ms")
        expectScroll(scrollDefinition).returning(getRequestFailureFuture())

        processor.scroll(someScrollId.get, someTimeout).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if documents in response have different scheme") {
        val scrollDefinition = ElasticDsl.searchScroll(someScrollId.get, someTimeout + "ms")

        expectScroll(scrollDefinition).returning(getRequestSuccessFuture(getBadResponse(Map(
          "key" -> 12.asInstanceOf[AnyRef]
        ))))

        processor.scroll(someScrollId.get, someTimeout).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }
  }

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }

  private def getRequestFailureFuture[T](status: Int = 500): Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(status, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }

  private def expectSearch(searchDefinition: SearchDefinition, isSizeSet: Boolean = false)
    (implicit client: HttpClient, elasticsearchConfig: ElasticsearchConfig) = {
    if (!isSizeSet)
      (elasticsearchConfig.getSearchPageSize _).expects().returning(defaultSizeValue)
    (client.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
      .expects(searchDefinition, SearchHttpExecutable, *)
  }

  private def expectScroll(searchScrollDefinition: SearchScrollDefinition)(implicit client: HttpClient) =
    (client.execute[SearchScrollDefinition, SearchResponse](_: SearchScrollDefinition)(_: HttpExecutable[SearchScrollDefinition, SearchResponse], _: ExecutionContext))
      .expects(searchScrollDefinition, SearchScrollHttpExecutable, *)

  private def getSearchResponse(histories: List[History], total: Int, scrollId: Option[String], sort: Seq[String] = Seq.empty) =
    SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), scrollId, Map.empty,
      SearchHits(total, 1, histories.zipWithIndex.map { case (value, key) => getSearchHit(key.toString, value, sort) }.toArray))

  private def getSearchHit(id: String, history: History, sortBy: Seq[String]) = {
    val source = Map("key" -> history.key, "value" -> history.value, "timestamp" -> history.timestamp.asInstanceOf[AnyRef],
      "operation" -> history.operation.toString)
    SearchHit(id, storageUuid, `type`, 1, 1, None, None, None, None, None, if (sortBy.isEmpty) None else Some(sortBy.map(source(_))), source, Map.empty, Map.empty, Map.empty)
  }

  private def getBadResponse(map: Map[String, AnyRef]) =
    SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), someScrollId, Map.empty,
      SearchHits(1, 1,
        Array(SearchHit("1", storageUuid, `type`, 1, 1, None, None, None, None, None, None, map, Map.empty, Map.empty, Map.empty))))

  private def getHistoryIndex(storage: String) = s"history-storage-$storage"

  private def getFields(history: KvHistory) = Map(
    "key" -> history.key,
    "value" -> history.value,
    "timestamp" -> history.timestamp,
    "operation" -> history.operation.toString)
}
