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

import com.bwsw.cloudstack.storage.kv.configuration.{AppConfig, ElasticsearchConfig}
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, NotFoundError}
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.delete.{DeleteByIdDefinition, DeleteByQueryDefinition}
import com.sksamuel.elastic4s.get.{GetDefinition, MultiGetDefinition}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem, BulkResponseItems}
import com.sksamuel.elastic4s.http.delete.{DeleteByQueryResponse, DeleteResponse}
import com.sksamuel.elastic4s.http.get.{GetResponse, MultiGetResponse}
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.search.{ClearScrollResponse, SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.searches.{ClearScrollDefinition, SearchDefinition, SearchScrollDefinition}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.AsyncFunSpec

import scala.concurrent.{ExecutionContext, Future}

class ElasticsearchKvProcessorSpec extends AsyncFunSpec with AsyncMockFactory {

  private val key = "someKey"
  private val value = "someValue"
  private val keyValues = Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")
  private val index = "storage-someStorage"
  private val storage = "someStorage"
  private val `type` = "_doc"
  private val valueField = "value"
  private val scrollSize = 1000
  private val exception = new RuntimeException("Test message")
  private val keepAlive = "1m"
  private val unlimitedLength = -1

  describe("An ElasticsearchKvProcessor") {
    val fakeClient = mock[HttpClient]
    val fakeConf = mock[AppConfig]
    val fakeEsConf = mock[ElasticsearchConfig]
    val elasticsearchKvProcessor = new ElasticsearchKvProcessor(fakeClient, fakeEsConf)

    describe("(get by the key)") {
      it("should return the value if the key exists") {
        val getResponse = GetResponse(key, index, `type`, 1, found = true, null, Map(valueField -> value))
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))

        elasticsearchKvProcessor.get(storage, key).map {
          case Right(result) => assert(result == value)
          case _ => fail
        }
      }

      it("should return NotFoundError if the key does not exist") {
        val getResponse = GetResponse(key, index, `type`, 1, found = false, null, Map.empty)
        expectGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))

        elasticsearchKvProcessor.get(storage, key).map {
          case Left(_: NotFoundError) => succeed
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectGetRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.get(storage, key)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectGetRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.get(storage, key).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(get by keys)") {
      it("should return values if keys exist") {
        val multiGetResponse = MultiGetResponse(keyValues.toList.map(kv => GetResponse(kv._1, index, `type`, 1, found = true, null, Map(valueField -> kv._2))))
        expectsMultiGetRequest(fakeClient).returning(getRequestSuccessFuture(multiGetResponse))

        elasticsearchKvProcessor.get(storage, keyValues.keys).map {
          case Right(values) => assert(values == keyValues.map(kv => kv._1 -> Some(kv._2)))
          case _ => fail
        }
      }

      it("should return None for keys that do not exist") {
        val noneKeyValues = keyValues + (keyValues.keys.head -> null)
        val multiGetResponse = MultiGetResponse(keyValues.toList.map(kv => {
          if (kv._2 == null)
            GetResponse(kv._1, index, `type`, 1, found = false, null, null)
          else
            GetResponse(kv._1, index, `type`, 1, found = true, null, Map(valueField -> kv._2))
        }))
        expectsMultiGetRequest(fakeClient).returning(getRequestSuccessFuture(multiGetResponse))

        elasticsearchKvProcessor.get(storage, keyValues.keys).map {
          case Right(values) => assert(values == keyValues.map(kv => {
            if (kv._2 == null)
              kv._1 -> None
            else
              kv._1 -> Some(kv._2)
          }))
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectsMultiGetRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.get(storage, keyValues.keys)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectsMultiGetRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.get(storage, keyValues.keys).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(set the key/value)") {
      def testSuccess(key: String, value: String, maxKeyLength: Int, maxValueLength: Int) = {
        val indexResponse = IndexResponse(key, index, `type`, 1, "created", forcedRefresh = false, null)
        expectsMaxKeyValueLength(fakeEsConf, key.length, value.length)
        expectsIndexRequest(fakeClient).returning(getRequestSuccessFuture(indexResponse))

        elasticsearchKvProcessor.set(storage, key, value).map {
          case Right(unit) => succeed
          case Left(error) => fail
        }
      }

      def testInvalidKey(key: String) = {
        expectsIndexRequest(fakeClient, key, value).never()

        elasticsearchKvProcessor.set(storage, key, value).map {
          case Left(error: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should set the value by the key") {
        testSuccess(key, value, key.length, value.length)
      }

      it("should set null value by the key") {
        val indexResponse = IndexResponse(key, index, `type`, 1, "created", forcedRefresh = false, null)
        (fakeEsConf.getMaxKeyLength _).expects().returning(key.length).atLeastOnce()
        expectsIndexRequest(fakeClient, key, null).returning(getRequestSuccessFuture(indexResponse))

        elasticsearchKvProcessor.set(storage, key, null).map {
          case Right(unit) => succeed
          case Left(error) => fail
        }
      }

      it("should set the value by the key if the value length is unlimited") {
        testSuccess(key, value, key.length, unlimitedLength)
      }

      it("should set the value by the key if the key length is unlimited") {
        testSuccess(key, value, unlimitedLength, value.length)
      }

      it("should fail if execute method fails") {
        expectsMaxKeyValueLength(fakeEsConf, key.length, value.length)
        expectsIndexRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.set(storage, key, value)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return BadRequest if the key is null") {
        testInvalidKey(null)
      }

      it("should return BadRequest if the key is empty") {
        testInvalidKey("")
      }

      it("should return BadRequest if the key is too long") {
        (fakeEsConf.getMaxKeyLength _).expects().returning(key.length - 1).atLeastOnce()
        testInvalidKey(key)
      }

      it("should return BadRequest if the value is too long") {
        expectsIndexRequest(fakeClient).never()
        expectsMaxKeyValueLength(fakeEsConf, key.length, value.length - 1)

        elasticsearchKvProcessor.set(storage, key, value).map {
          case Left(error: BadRequestError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if the request fails") {
        expectsMaxKeyValueLength(fakeEsConf, key.length, value.length)
        expectsIndexRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.set(storage, key, value).map {
          case Left(error: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(set key/value pairs)") {
      def testSuccess(keyValues: Map[String, String], maxKeyLength: Int, maxValueLength: Int) = {
        val bulkResponse = BulkResponse(1, errors = false,
          keyValues.toList.map(kv => BulkResponseItems(
            Some(BulkResponseItem(0, kv._1, index, `type`, 1, forcedRefresh = false, found = false, created = false, "created", 200, None, None)),
            None,
            None,
            None)))
        expectsMaxKeyValueLength(fakeEsConf, maxKeyLength, maxValueLength)
        expectsBulkIndexRequest(fakeClient, keyValues).returning(getRequestSuccessFuture(bulkResponse))

        elasticsearchKvProcessor.set(storage, keyValues).map {
          case Right(map) => assert(map.forall(v => v._2))
          case _ => fail
        }
      }

      def testInvalidKey(key: String) = {
        elasticsearchKvProcessor.set(storage, Map(key -> value)).map {
          case Right(map) => assert(map.forall(v => !v._2))
          case _ => fail
        }
      }

      it("should set values by keys") {
        testSuccess(keyValues, getMaxLength(keyValues.keys), getMaxLength(keyValues.values))
      }

      it("should set null values by keys") {
        val data = keyValues ++ Map("nullValue" -> null)
        testSuccess(data, getMaxLength(data.keys), getMaxLength(data.values))
      }

      it("should set values by keys if the value length is unlimited") {
        testSuccess(keyValues, getMaxLength(keyValues.keys), unlimitedLength)
      }

      it("should set values by keys if the key length is unlimited") {
        testSuccess(keyValues, unlimitedLength, getMaxLength(keyValues.values))
      }

      it("should not set too long values by keys") {
        val bulkResponse = BulkResponse(1, errors = false,
          keyValues.toList.map(kv => BulkResponseItems(
            Some(BulkResponseItem(0, kv._1, index, `type`, 1, forcedRefresh = false, found = false, created = false, "created", 200, None, None)),
            None,
            None,
            None)))

        val maxValueLength = getMaxLength(keyValues.values)
        expectsMaxKeyValueLength(fakeEsConf, getMaxLength(keyValues.keys), maxValueLength)
        expectsBulkIndexRequest(fakeClient, keyValues).returning(getRequestSuccessFuture(bulkResponse))

        val invalidKeyValues = Map("keyForLongValue" -> "a" * (maxValueLength + 1))
        elasticsearchKvProcessor.set(storage, keyValues ++ invalidKeyValues).map {
          case Right(map) =>
            assert(keyValues.map(kv => (kv._1, true)) ++ invalidKeyValues.map(kv => (kv._1, false)) == map)
          case _ => fail
        }
      }

      it("should return false for null keys") {
        testInvalidKey(null)
      }

      it("should return false for empty keys") {
        testInvalidKey("")
      }

      it("should return false for too long keys") {
        (fakeEsConf.getMaxKeyLength _).expects().returning(key.length - 1).atLeastOnce()
        testInvalidKey(key)
      }

      it("should fail if execute method fails") {
        expectsMaxKeyValueLength(fakeEsConf, getMaxLength(keyValues.keys), getMaxLength(keyValues.values))
        expectsBulkIndexRequest(fakeClient, keyValues).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.set(storage, keyValues)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectsMaxKeyValueLength(fakeEsConf, getMaxLength(keyValues.keys), getMaxLength(keyValues.values))
        expectsBulkIndexRequest(fakeClient, keyValues).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.set(storage, keyValues).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(delete by the key)") {
      it("should delete the value by the key") {
        val deleteResponse = DeleteResponse(Shards(1, 0, 1), index, `type`, key, 1, "deleted")
        expectsDeleteRequest(fakeClient).returning(getRequestSuccessFuture(deleteResponse))

        elasticsearchKvProcessor.delete(storage, key).map {
          case Right(unit) => succeed
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectsDeleteRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.delete(storage, key)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectsDeleteRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.delete(storage, key).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(delete by keys)") {
      it("should delete values by keys") {
        val bulkResponse = BulkResponse(1, errors = false,
          keyValues.toList.map(kv => BulkResponseItems(
            None,
            Some(BulkResponseItem(1, kv._1, index, `type`, 1, forcedRefresh = false, found = false, created = false, "deleted", 200, None, None)),
            None,
            None)))
        expectsBulkDeleteRequest(fakeClient).returning(getRequestSuccessFuture(bulkResponse))

        elasticsearchKvProcessor.delete(storage, keyValues.keys).map {
          case Right(map) => assert(map.forall(v => v._2))
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectsBulkDeleteRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.delete(storage, keyValues.keys)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectsBulkDeleteRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.delete(storage, keyValues.keys).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(list)") {
      val scrollId1 = "one"
      val scrollId2 = "two"
      val scrollId3 = "three"
      val hits = keyValues.toList.map(kv => SearchHit(kv._1, index, `type`, 1, 1, None, None, None, None, None, None, Map(valueField -> kv._2), null, null, null)).toArray
      val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), Some(scrollId1), Map.empty, SearchHits(hits.length, 1,
        hits.slice(0, hits.length - 1)))

      it("should return existing keys") {
        val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), None, Map.empty, SearchHits(hits.length, 1, hits))
        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).returning(getRequestSuccessFuture(searchResponse))
        elasticsearchKvProcessor.list(storage).map {
          case Right(values) => assert(keyValues.keys.toList == values)
          case _ => fail
        }
      }

      it("should return all existing keys if scrolling is required") {
        val searchScrollResponse1 = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1),
          Some(scrollId2), Map.empty, SearchHits(hits.length, 1, hits.slice(hits.length - 1, hits.length)))

        val searchScrollResponse2 = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1),
          Some(scrollId3), Map.empty, SearchHits(hits.length, 1, Array()))

        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).returning(getRequestSuccessFuture(searchResponse))
        expectsSearchScrollRequest(fakeClient, fakeConf, scrollId1).returning(getRequestSuccessFuture(searchScrollResponse1))
        expectsSearchScrollRequest(fakeClient, fakeConf, scrollId2).returning(getRequestSuccessFuture(searchScrollResponse2))
        expectsClearScrollRequest(fakeClient, scrollId3).returning(getRequestSuccessFuture(ClearScrollResponse(succeeded = true, 1)))
        elasticsearchKvProcessor.list(storage).map {
          case Right(values) => assert(keyValues.keys.toList == values)
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.list(storage)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should fail if scrolling fails") {
        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).returning(getRequestSuccessFuture(searchResponse))
        expectsSearchScrollRequest(fakeClient, fakeConf, scrollId1).throwing(new Exception())
        recoverToSucceededIf[Exception] {
          elasticsearchKvProcessor.list(storage)
        }
      }

      it("should return InternalError if the request fails") {
        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.list(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }

      it("should return InternalError if scrolling fails") {
        expectsSearchRequest(fakeClient, fakeEsConf, scrollSize).returning(getRequestSuccessFuture(searchResponse))
        expectsSearchScrollRequest(fakeClient, fakeConf, scrollId1).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.list(storage).map {
          case Left(_: InternalError) => succeed
          case _ => fail
        }
      }
    }

    describe("(clear)") {
      it("should remove all key/value pairs") {
        val deleteByQueryResponse = DeleteByQueryResponse(1, timedOut = false, 3, 3, 1, 0, 0, 0, 3, 0)
        expectsDeleteByQueryRequest(fakeClient).returning(getRequestSuccessFuture(deleteByQueryResponse))
        elasticsearchKvProcessor.clear(storage).map {
          case Right(unit) => succeed
          case _ => fail
        }
      }

      it("should fail if execute method fails") {
        expectsDeleteByQueryRequest(fakeClient).throwing(exception)

        recoverToExceptionIf[Exception] {
          Future {
            elasticsearchKvProcessor.clear(storage)
          }
        } map { ex => assert(ex == exception) }
      }

      it("should return InternalError if the request fails") {
        expectsDeleteByQueryRequest(fakeClient).returning(getRequestFailureFuture)
        elasticsearchKvProcessor.clear(storage).map {
          case Left(error: InternalError) => succeed
          case _ => fail
        }
      }
    }
  }

  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }

  private def getRequestFailureFuture[T]: Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }

  private def expectGetRequest(client: HttpClient) = {
    (client.execute[GetDefinition, GetResponse](_: GetDefinition)(_: HttpExecutable[GetDefinition, GetResponse], _: ExecutionContext))
      .expects(ElasticDsl.get(key).from(index / `type`), GetHttpExecutable, *)
  }

  private def expectsMultiGetRequest(client: HttpClient) = {
    val gets = keyValues.toList.map(kv => ElasticDsl.get(kv._1).from(index, `type`))
    (client.execute[MultiGetDefinition, MultiGetResponse](_: MultiGetDefinition)(_: HttpExecutable[MultiGetDefinition, MultiGetResponse], _: ExecutionContext))
      .expects(multiget(gets), MultiGetHttpExecutable, *)
  }

  private def expectsIndexRequest(client: HttpClient) = {
    (client.execute[IndexDefinition, IndexResponse](_: IndexDefinition)(_: HttpExecutable[IndexDefinition, IndexResponse], _: ExecutionContext))
      .expects(indexInto(index / `type`) id key fields (valueField -> value), IndexHttpExecutable, *)
  }

  private def expectsIndexRequest(client: HttpClient, key: String, value: String) = {
    (client.execute[IndexDefinition, IndexResponse](_: IndexDefinition)(_: HttpExecutable[IndexDefinition, IndexResponse], _: ExecutionContext))
      .expects(indexInto(index / `type`) id key fields (valueField -> value), IndexHttpExecutable, *)
  }

  private def expectsBulkIndexRequest(client: HttpClient, kvs: Map[String, String]) = {
    val sets = kvs.toList.map(kv => indexInto(index / `type`) id kv._1 fields (valueField -> kv._2))
    (client.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
      .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
  }

  private def expectsDeleteRequest(client: HttpClient) = {
    (client.execute[DeleteByIdDefinition, DeleteResponse](_: DeleteByIdDefinition)(_: HttpExecutable[DeleteByIdDefinition, DeleteResponse], _: ExecutionContext))
      .expects(deleteById(index, `type`, key), DeleteByIdExecutable, *)
  }

  private def expectsBulkDeleteRequest(client: HttpClient) = {
    val deletes = keyValues.keySet.map(k => deleteById(index, `type`, k))
    (client.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
      .expects(ElasticDsl.bulk(deletes), BulkExecutable, *)
  }

  private def expectsSearchRequest(client: HttpClient, conf: ElasticsearchConfig, size: Int) = {
    (conf.getScrollPageSize _).expects().returning(size)
    (conf.getScrollKeepAlive _).expects().returning(keepAlive)
    (client.execute[SearchDefinition, SearchResponse](_: SearchDefinition)(_: HttpExecutable[SearchDefinition, SearchResponse], _: ExecutionContext))
      .expects(ElasticDsl.search(index).size(size).keepAlive(keepAlive), SearchHttpExecutable, *)
  }

  private def expectsSearchScrollRequest(client: HttpClient, conf: AppConfig, scrollId: String) = {
    (client.execute[SearchScrollDefinition, SearchResponse](_: SearchScrollDefinition)(_: HttpExecutable[SearchScrollDefinition, SearchResponse], _: ExecutionContext))
      .expects(searchScroll(scrollId).keepAlive(keepAlive), SearchScrollHttpExecutable, *)
  }

  private def expectsDeleteByQueryRequest(client: HttpClient) = {
    (client.execute[DeleteByQueryDefinition, DeleteByQueryResponse](_: DeleteByQueryDefinition)(_: HttpExecutable[DeleteByQueryDefinition, DeleteByQueryResponse], _: ExecutionContext))
      .expects(deleteByQuery(index, `type`, matchAllQuery).proceedOnConflicts(true), DeleteByQueryExecutable, *)
  }

  private def expectsClearScrollRequest(client: HttpClient, scrollId: String) = {
    (client.execute[ClearScrollDefinition, ClearScrollResponse](_: ClearScrollDefinition)(_: HttpExecutable[ClearScrollDefinition, ClearScrollResponse], _: ExecutionContext))
      .expects(clearScroll(scrollId), ClearScrollHttpExec, *)
  }

  private def expectsMaxKeyValueLength(conf: ElasticsearchConfig, maxKeyLength: Int, maxValueLength: Int) = {
    (conf.getMaxKeyLength _).expects().returning(maxKeyLength).atLeastOnce()
    (conf.getMaxValueLength _).expects().returning(maxValueLength).atLeastOnce()
  }

  private def getMaxLength(data: Iterable[String]): Int = data.map(e => if (e == null) 0 else e.length).max

}
