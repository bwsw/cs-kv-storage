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

import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, KvHistory, Set}
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.ElasticDsl.{indexInto, _}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkError, BulkResponse, BulkResponseItem, BulkResponseItems}
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

  describe("An ElasticsearchHistoryProcessor") {
    val fakeClient = mock[HttpClient]
    val processor = new ElasticsearchHistoryProcessor(fakeClient)

    describe("(save histories)") {
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

        processor.save(histories).map {
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
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Right(RequestSuccess(200, Option.empty, Map.empty, bulkResponse))))

        processor.save(histories).map {
          case None => fail
          case Some(List(history1)) =>
            if (histories(1).equals(history1))
              succeed
            else
              fail
        }
      }

      it("should return all given histories if request fails") {
        val sets = histories.map(history => indexInto(getHistoryIndex(history.storage) / `type`) fields getFields(history))
        (fakeClient.execute[BulkDefinition, BulkResponse](_: BulkDefinition)(_: HttpExecutable[BulkDefinition, BulkResponse], _: ExecutionContext))
          .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
          .returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
        processor.save(histories).map {
          case Some(list) =>
            if (histories.diff(list).isEmpty)
              succeed
            else
              fail
          case None => fail
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
