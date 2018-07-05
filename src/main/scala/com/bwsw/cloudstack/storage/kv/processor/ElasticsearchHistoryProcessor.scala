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
import com.bwsw.cloudstack.storage.kv.entity._
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message._
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils
import com.sksamuel.elastic4s.http.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}
import com.sksamuel.elastic4s.http.search.SearchHits
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortOrder}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** A processor of storage histories stored in Elasticsearch
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchHistoryProcessor(
    client: HttpClient,
    elasticsearchConfig: ElasticsearchConfig) extends HistoryProcessor {

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

  def get(
      storageUuid: String,
      keys: Iterable[String],
      operations: Iterable[Operation],
      start: Option[Long],
      end: Option[Long],
      sort: Iterable[String],
      page: Option[Int],
      size: Option[Int],
      scroll: Option[Long]): Future[Either[StorageError, SearchResponseBody[History]]] = {

    var searchDef: SearchDefinition = search(getHistoricalStorage(storageUuid))
    val queries: ArrayBuffer[QueryDefinition] = if (keys.nonEmpty) ArrayBuffer(termsQuery(
      "key",
      keys)) else ArrayBuffer.empty

    if (operations.nonEmpty)
      queries += termsQuery("operation", operations.map(_.toString))

    (start, end) match {
      case (None, None) => None
      case (None, Some(e)) => queries += rangeQuery("timestamp").lte(e)
      case (Some(s), None) => queries += rangeQuery("timestamp").gte(s)
      case (Some(s), Some(e)) =>
        if (s == e)
          queries += termQuery("timestamp", s)
        else
          queries += rangeQuery("timestamp").gte(s).lte(e)
    }

    searchDef = queries.size match {
      case 0 => searchDef
      case 1 => searchDef.query(queries.head)
      case _ => searchDef.query(boolQuery().filter(queries))
    }

    val pageSize = size.getOrElse(elasticsearchConfig.getScrollPageSize)
    searchDef = searchDef.size(pageSize)

    if (page.nonEmpty) {
      searchDef = searchDef.from(pageSize * (page.get - 1))
    }

    if (scroll.nonEmpty) {
      searchDef = searchDef.scroll(scroll.get + "ms")
    }

    if (sort.nonEmpty)
      searchDef = searchDef.sortBy(sort.map { field =>
        if (field.head == '-')
          FieldSortDefinition(field.substring(1), order = SortOrder.DESC)
        else
          FieldSortDefinition(field, order = SortOrder.ASC)
      })

    client.execute(searchDef).map {
      case Left(failure) => Left(ElasticsearchUtils.getError(failure))
      case Right(success) =>
        try {
          success.result.scrollId match {
            case Some(scrollId) =>
              Right(SearchScrolledBody(
                success.result.totalHits,
                success.result.size,
                scrollId,
                getItems(success.result.hits)
              ))
            case None =>
              Right(SearchPagedBody(
                success.result.totalHits,
                success.result.size,
                page.getOrElse(1),
                getItems(success.result.hits)
              ))
          }
        }
        catch {
          case iae: IllegalArgumentException => Left(InternalError(iae.getMessage))
        }
    }
  }

  def scroll(scrollId: String, timeout: Long): Future[Either[StorageError, SearchScrolledBody[History]]] = {
    client.execute(searchScroll(scrollId).keepAlive(timeout + "ms"))
      .map {
        case Left(RequestFailure(404, _, _, _)) =>
          Left(BadRequestError())
        case Left(RequestFailure(400, _, _, _)) =>
          Left(BadRequestError())
        case Left(failure) =>
          Left(ElasticsearchUtils.getError(failure))
        case Right(success) =>
          try {
            Right(SearchScrolledBody(
              success.result.totalHits,
              success.result.size,
              success.result.scrollId.get,
              getItems(success.result.hits)
            ))
          }
          catch {
            case iae: IllegalArgumentException => Left(InternalError(iae.getMessage))
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
      case _ => throw new IllegalArgumentException("Invalid String result")
    }
  }

  protected def getLong(name: String)(implicit fields: Map[String, Any]): Long = {
    fields.get(name) match {
      case Some(value: Long) => value
      case Some(value: Int) => value
      case _ => throw new IllegalArgumentException("Invalid Long result")
    }
  }

  protected def getItems(searchHits: SearchHits): List[History] = searchHits.hits.map {
    hit =>
      implicit val fields: Map[String, Any] = hit.sourceAsMap
      History(getString("key"), getString("value"), getLong("timestamp"), Operation.parse(getString("operation")))
  }.toList

}
