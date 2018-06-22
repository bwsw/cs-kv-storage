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

import com.bwsw.cloudstack.storage.kv.entity.History
import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, HistoryPagedBody, HistoryResponseBody, HistoryScrolledBody, KvHistory, Operation, Set}
import com.bwsw.cloudstack.storage.kv.processor.ElasticsearchKvProcessor.getIds
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils
import com.sksamuel.elastic4s.http.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHits
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortOrder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** A processor of storage histories stored in Elasticsearch
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchHistoryProcessor(client: HttpClient) extends HistoryProcessor {

  import ElasticsearchHistoryProcessor._

  def save(histories: List[KvHistory]): Future[Option[List[KvHistory]]] = {
    val indices = histories.map {
      record =>
        indexInto(getHistoricalStorage(record.storage), `type`) fields getFields(record)
    }
    client.execute(bulk(indices)).map {
      case Left(_) => Some(histories)
      case Right(success) =>
        val erroneous = success.result.items.filter(_.error.isDefined).map(item => histories(item.itemId))
        if (erroneous.isEmpty)
          None
        else
          Some(erroneous.toList)
    }
  }

  def get(storageUuid: String,
          keys: Iterable[String],
          operations: Iterable[Operation],
          start: Long,
          end: Long,
          sort: Iterable[String],
          page: Int,
          size: Int,
          scroll: Int): Future[Either[StorageError, HistoryResponseBody]] = {

    val searchDef = search(getHistoricalStorage(storageUuid))
    val withKeys = if (keys.nonEmpty) Seq(termsQuery("key", keys)) else Seq.empty
    val withOperations = if (operations.nonEmpty) withKeys :+ termsQuery("operation", operations.map(_.toString)) else withKeys

    val queries =
      if (start == 0)
        if (end == 0)
          withOperations
        else
          withOperations :+ rangeQuery("timestamp").lte(end)
      else if (end == 0)
        withOperations :+ rangeQuery("timestamp").gte(start)
      else if (end == start)
        withOperations :+ termQuery("timestamp", start)
      else
        withOperations :+ rangeQuery("timestamp").gte(start).lte(end)

    val queryDef = queries.size match {
      case 0 => searchDef
      case 1 => searchDef.query(queries.head)
      case _ => searchDef.query(boolQuery().filter(queries))
    }

    val withSize = queryDef.size(size)
    val withPage =
      if (page > 1)
        withSize.from(size * (page - 1))
      else
        withSize

    val withScroll =
      if (scroll > 0)
        withPage.scroll(scroll + "ms")
      else
        withPage

    val withSort =
      if (sort.nonEmpty)
        withScroll.sortBy(sort.map { field =>
          if (field.head == '-')
            FieldSortDefinition(field.substring(1), order = SortOrder.DESC)
          else
            FieldSortDefinition(field, order = SortOrder.ASC)
        })
      else
        withScroll

    client.execute(withSort).map {
      case Left(failure) => Left(ElasticsearchUtils.getError(failure))
      case Right(success) =>
        try {
          success.result.scrollId match {
            case Some(scrollId) =>
              Right(HistoryScrolledBody(
                success.result.totalHits,
                success.result.size,
                scrollId,
                getItems(success.result.hits)
              ))
            case None =>
              Right(HistoryPagedBody(
                success.result.totalHits,
                success.result.size,
                page,
                getItems(success.result.hits)
              ))
          }
        }
        catch {
          case ParseError(message) => Left(InternalError(message))
        }
    }
  }

  def scroll(scrollId: String, timeout: Long): Future[Either[StorageError, HistoryScrolledBody]] = {
    client.execute(searchScroll(scrollId).keepAlive(timeout + "ms"))
      .map {
        case Left(failure) => Left(ElasticsearchUtils.getError(failure))
        case Right(success) =>
          try {
            Right(HistoryScrolledBody(
              success.result.totalHits,
              success.result.size,
              success.result.scrollId.get,
              getItems(success.result.hits)
            ))
          }
          catch {
            case ParseError(message) => Left(InternalError(message))
          }
      }
  }
}

object ElasticsearchHistoryProcessor {
  protected val `type` = "_doc"

  protected def getHistoricalStorage(storageUuid: String): String = {
    s"history-storage-$storageUuid"
  }

  protected def getFields(history: KvHistory): Map[String, Any] = {
    Map(
      "key" -> history.key,
      "value" -> history.value,
      "timestamp" -> history.timestamp,
      "operation" -> history.operation.toString)
  }

  protected def getString(name: String)(implicit fields: Map[String, Any]): String = {
    fields.get(name) match {
      case Some(null) => null
      case Some(value: String) => value
      case _ => throw ParseError("Invalid String result")
    }
  }

  protected def getLong(name: String)(implicit fields: Map[String, Any]): Long = {
    fields.get(name) match {
      case Some(value: Long) => value
      case Some(value: Int) => value
      case _ => throw ParseError("Invalid Long result")
    }
  }

  protected def getOperation(string: String): Operation = string match {
    case "set" => Set
    case "delete" => Delete
    case "clear" => Clear
    case _ => throw ParseError("Invalid Operation result")
  }

  protected def getItems(searchHits: SearchHits): List[History] = searchHits.hits.map {
    hit =>
      implicit val fields: Map[String, Any] = hit.sourceAsMap
      History(getString("key"), getString("value"), getLong("timestamp"), getOperation(getString("operation")))
  }.toList

  private case class ParseError(message: String) extends Exception

}
