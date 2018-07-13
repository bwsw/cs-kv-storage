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

import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.entity.Operation.{Clear, Delete, Set}
import com.bwsw.cloudstack.storage.kv.entity.{History, Operation, PageSearchResult, ScrollSearchResult, SearchResult,
  SortField, Sorting}
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.KvHistory
import com.bwsw.cloudstack.storage.kv.util.elasticsearch._
import com.bwsw.cloudstack.storage.kv.util.test._
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.{indexInto, _}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkError, BulkResponse, BulkResponseItem, BulkResponseItems}
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortOrder}
import com.sksamuel.elastic4s.searches.{SearchDefinition, SearchScrollDefinition}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest
import org.scalatest.AsyncFunSpec

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchHistoryProcessorSpec extends AsyncFunSpec with AsyncMockFactory {

  private val artificialKey = "cbIaz2MBpp4Ypizt4vT5"
  private val scrollId = Some("DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAAcWVDBqc3Vkb3lUeDZOYXk4bWczTHowUQ==")
  private val defaultSizeValue = 50
  private val storageUuid = "someStorage"
  private val someKey = "someKey"
  private val someValue = "someValue"
  private val someAnyRefNumber = 1.asInstanceOf[AnyRef]
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
    implicit val appConfig: AppConfig = mock[AppConfig]
    val processor = new ElasticsearchHistoryProcessor(fakeClient, appConfig)

    describe("(save histories)") {
      it("should save a list of historical records") {
        val bulkResponse = BulkResponse(
          1,
          errors = false,
          kvHistories.map(kvHistory => BulkResponseItems(
            Some(BulkResponseItem(
              0,
              artificialKey,
              getHistoricalStorageIndex(kvHistory.storage),
              DocumentType,
              1,
              forcedRefresh = false,
              found = false,
              created = true,
              "created",
              200,
              None,
              None)), None, None, None)))
        val sets = kvHistories
          .map(history => indexInto(getHistoricalStorageIndex(history.storage) / DocumentType) fields getFields(
            history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(
          _: HttpExecutable[BulkDefinition, BulkResponse],
          _: ExecutionContext)).expects(ElasticDsl.bulk(sets), BulkExecutable, *)
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
              0,
              artificialKey,
              getHistoricalStorageIndex("someStorage2"),
              DocumentType,
              1,
              forcedRefresh = false,
              found = false,
              created = true,
              "created",
              200,
              None,
              None)), None, None, None),
          BulkResponseItems(
            Some(BulkResponseItem(
              1,
              artificialKey,
              getHistoricalStorageIndex("someStorage"),
              DocumentType,
              1,
              forcedRefresh = false,
              found = false,
              created = true,
              "created",
              500,
              Some(BulkError("", "", "", 1, getHistoricalStorageIndex("someStorage"))),
              None)), None, None, None),
          BulkResponseItems(
            Some(BulkResponseItem(
              2,
              artificialKey,
              getHistoricalStorageIndex("someStorage2"),
              DocumentType,
              1,
              forcedRefresh = false,
              found = false,
              created = true,
              "created",
              200,
              None,
              None)), None, None, None))
        val bulkResponse = BulkResponse(1, errors = true, bulkResponseItems)
        val sets = kvHistories
          .map(kvHistory => indexInto(getHistoricalStorageIndex(kvHistory.storage) / DocumentType) fields getFields(
            kvHistory))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(
          _: HttpExecutable[BulkDefinition, BulkResponse],
          _: ExecutionContext)).expects(ElasticDsl.bulk(sets), BulkExecutable, *)
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

      it("should return all given histories if request fails") {
        val sets = kvHistories
          .map(kvHistory => indexInto(getHistoricalStorageIndex(kvHistory.storage) / DocumentType) fields getFields(
            kvHistory))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(
          _: HttpExecutable[BulkDefinition, BulkResponse],
          _: ExecutionContext)).expects(ElasticDsl.bulk(sets), BulkExecutable, *).returning(Future(Left(RequestFailure(
          404,
          Option.empty,
          Map.empty,
          ElasticError.fromThrowable(new RuntimeException())))))
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

      val searchResponse = getSearchResponse(historyList, historyList.size, None)
      val result = PageSearchResult(historyList.size, historyList.size, 1, historyList)

      def test(
          searchDefinition: SearchDefinition,
          start: Option[Long],
          end: Option[Long]): Future[scalatest.Assertion] = {
        expectSearch(searchDefinition.size(defaultSizeValue)).returning(getRequestSuccessFuture(searchResponse))

        processor.get(
          storageUuid,
          immutable.Set.empty[String],
          immutable.Set.empty[Operation],
          start,
          end,
          immutable.Set.empty[SortField],
          None,
          None,
          None).map {
          case Right(response) => assert(response == result)
          case _ => fail
        }
      }

      def testBadResponse(searchResponse: SearchResponse): Future[scalatest.Assertion] = {
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))
        expectSearch(searchDefinition.size(defaultSizeValue)).returning(getRequestSuccessFuture(searchResponse))

        getByStorage(storageUuid).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      def getByStorage(storageUuid: String): Future[Either[StorageError, SearchResult[History]]] = processor.get(
        storageUuid,
        immutable.Set.empty[String],
        immutable.Set.empty[Operation],
        None,
        None,
        immutable.Set.empty[SortField],
        None,
        None,
        None)

      it("should return values by the storage uuid only") {
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))

        test(searchDefinition, None, None)
      }

      it("should retrieve by timestamp start") {
        val start = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))
          .query(rangeQuery(HistoryFields.Timestamp).gte(start.get))

        test(searchDefinition, start, None)
      }

      it("should retrieve by timestamp end") {
        val end = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))
          .query(rangeQuery(HistoryFields.Timestamp).lte(end.get))

        test(searchDefinition, None, end)
      }

      it("should retrieve by different timestamp start and end") {
        val start = Some(1.asInstanceOf[Long])
        val end = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))
          .query(rangeQuery(HistoryFields.Timestamp).gte(start.get).lte(end.get))

        test(searchDefinition, start, end)
      }

      it("should retrieve by same timestamp start and end") {
        val startAndEnd = Some(2.asInstanceOf[Long])
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid))
          .query(termQuery(HistoryFields.Timestamp, startAndEnd.get))

        test(searchDefinition, startAndEnd, startAndEnd)
      }

      it("should retrieve by all parameters") {
        val pageSize = Some(3)
        val page = Some(2)
        val keys = immutable.Set("key1")
        val operations: immutable.Set[Operation] = immutable.Set(Set)
        val start = Some(1.asInstanceOf[Long])
        val end = Some(2.asInstanceOf[Long])
        val scroll = Some(2000.asInstanceOf[Long])
        val sort = immutable
          .Set(SortField(HistoryFields.Key, Sorting.Asc), SortField(HistoryFields.Timestamp, Sorting.Desc))

        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid)).
          size(pageSize.get).scroll(scroll.get + ScrollTimeoutUnit).query(boolQuery().filter(List(
          termsQuery(HistoryFields.Key, keys),
          termsQuery(HistoryFields.Operation, operations.map(_.toString)),
          rangeQuery(HistoryFields.Timestamp).gte(start.get).lte(end.get)))).
          from(pageSize.get * (page.get - 1)).sortBy(List(
          FieldSortDefinition(HistoryFields.Key, order = SortOrder.ASC),
          FieldSortDefinition(HistoryFields.Timestamp, order = SortOrder.DESC)))
        val total = historyList.size + 1
        val searchResponse = getSearchResponse(historyList, total, scrollId)

        expectSearch(searchDefinition, isSizeSet = true).returning(getRequestSuccessFuture(searchResponse))

        processor.get(storageUuid, keys, operations, start, end, sort, page, pageSize, scroll)
          .map {
            case Right(response) =>
              assert(response == ScrollSearchResult(total, pageSize.get.toLong, scrollId.get, historyList))
            case _ => fail
          }
      }

      it("should return InternalError if Elasticsearch request fails") {
        val searchDefinition = ElasticDsl.search(getHistoricalStorageIndex(storageUuid)).size(defaultSizeValue)
        expectSearch(searchDefinition).returning(getRequestFailureFuture[SearchResponse]())

        getByStorage(storageUuid).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if a bad value of string field is in Elasticsearch response") {
        val searchResponse = getBadResponse(Map(
          HistoryFields.Key -> someKey,
          HistoryFields.Value -> someAnyRefNumber,
          HistoryFields.Timestamp -> someAnyRefNumber,
          HistoryFields.Operation -> Set.toString))
        testBadResponse(searchResponse)
      }

      it("should return InternalError if a bad value of long field is in Elasticsearch response") {
        val searchResponse = getBadResponse(Map(
          HistoryFields.Key -> someKey,
          HistoryFields.Value -> someValue,
          HistoryFields.Timestamp -> "1",
          HistoryFields.Operation -> Set.toString))
        testBadResponse(searchResponse)
      }

      it("should return InternalError if invalid Operation value is in Elasticsearch response") {
        val searchResponse = getBadResponse(Map(
          HistoryFields.Key -> someKey,
          HistoryFields.Value -> someValue,
          HistoryFields.Timestamp -> someAnyRefNumber,
          HistoryFields.Operation -> "operation123"))
        testBadResponse(searchResponse)
      }
    }

    describe("(scroll by request)") {

      val timeout = 1000

      def test(
          response: Future[Either[RequestFailure, RequestSuccess[SearchResponse]]],
          f: Either[StorageError, ScrollSearchResult[History]] => scalatest.Assertion): Future[scalatest.Assertion] = {
        val scrollDefinition = ElasticDsl.searchScroll(scrollId.get, timeout + ScrollTimeoutUnit)
        expectScroll(scrollDefinition).returning(response)

        processor.scroll(scrollId.get, timeout).map(f(_))
      }

      it("should return next batch of results") {
        val total = 5
        val searchScrollResponse = getSearchResponse(historyList, total, scrollId, Seq.empty)

        test(
          getRequestSuccessFuture(searchScrollResponse), {
            case Right(result) => assert(result == ScrollSearchResult(
              total,
              historyList.size,
              scrollId.get,
              historyList))
            case _ => fail
          })
      }

      it("should return BadRequestError if scroll has expired or doesn't exist") {
        test(
          getRequestFailureFuture[SearchResponse](404), {
            case Left(_: BadRequestError) => succeed
            case _ => fail
          })
      }

      it("should return BadRequestError if scrollId is invalid") {
        test(
          getRequestFailureFuture[SearchResponse](400), {
            case Left(_: BadRequestError) => succeed
            case _ => fail
          })
      }

      it("should return InternalError if Elasticsearch request fails") {
        test(
          getRequestFailureFuture[SearchResponse](), {
            case Left(_: InternalError) => succeed
            case _ => fail
          })
      }

      it("should return InternalError if documents in Elasticsearch response have a different scheme") {
        test(
          getRequestSuccessFuture(getBadResponse(Map(
            "key" -> someAnyRefNumber
          ))), {
            case Left(_: InternalError) => succeed
            case _ => fail
          })
      }
    }
  }

  private def expectSearch(searchDefinition: SearchDefinition, isSizeSet: Boolean = false)
    (implicit client: HttpClient, appConfig: AppConfig) = {
    if (!isSizeSet)
      (appConfig.getDefaultPageSize _).expects().returning(defaultSizeValue)
    (client.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(
      _: HttpExecutable[SearchDefinition, SearchResponse],
      _: ExecutionContext))
      .expects(searchDefinition, SearchHttpExecutable, *)
  }

  private def expectScroll(searchScrollDefinition: SearchScrollDefinition)(implicit client: HttpClient) =
    (client.execute[SearchScrollDefinition, SearchResponse](_: SearchScrollDefinition)(
      _: HttpExecutable[SearchScrollDefinition, SearchResponse],
      _: ExecutionContext))
      .expects(searchScrollDefinition, SearchScrollHttpExecutable, *)

  private def getSearchResponse(
      histories: List[History],
      total: Int,
      scrollId: Option[String],
      sort: Seq[String] = Seq.empty) = SearchResponse(
    1,
    isTimedOut = false,
    isTerminatedEarly = false,
    Map.empty,
    Shards(1, 0, 1),
    scrollId,
    Map.empty,
    SearchHits(
      total,
      1,
      histories.zipWithIndex.map { case (value, key) => getSearchHit(
        key.toString,
        value,
        sort)
      }.toArray))

  private def getSearchHit(id: String, history: History, sortBy: Seq[String]) = {
    val source = Map(
      HistoryFields.Key -> history.key,
      HistoryFields.Value -> history.value,
      HistoryFields.Timestamp -> history.timestamp.asInstanceOf[AnyRef],
      HistoryFields.Operation -> history.operation.toString)
    SearchHit(
      id,
      storageUuid,
      DocumentType,
      1,
      1,
      None,
      None,
      None,
      None,
      None,
      if (sortBy.isEmpty) None else Some(sortBy.map(source(_))),
      source,
      Map.empty,
      Map.empty,
      Map.empty)
  }

  private def getBadResponse(map: Map[String, AnyRef]) =
    SearchResponse(
      1,
      isTimedOut = false,
      isTerminatedEarly = false,
      Map.empty,
      Shards(1, 0, 1),
      scrollId,
      Map.empty,
      SearchHits(
        1,
        1,
        Array(SearchHit(
          "1",
          storageUuid,
          DocumentType,
          1,
          1,
          None,
          None,
          None,
          None,
          None,
          None,
          map,
          Map.empty,
          Map.empty,
          Map.empty))))

  private def getFields(history: KvHistory) = Map(
    HistoryFields.Key -> history.key,
    HistoryFields.Value -> history.value,
    HistoryFields.Timestamp -> history.timestamp,
    HistoryFields.Operation -> history.operation.toString)
}
