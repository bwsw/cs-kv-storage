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
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils._
import com.sksamuel.elastic4s.http.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.search.{SearchHits, SearchResponse}
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
        indexInto(getHistoricalStorageIndex(record.storage), `type`) fields getFields(record)
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

    implicit val esConfig: ElasticsearchConfig = elasticsearchConfig
    implicit val esClient: HttpClient = client
    val search =
      Search
        .storage(storageUuid)
        .keys(keys)
        .operations(operations)
        .time(start, end)
        .sort(sort)
        .page(page)
        .size(size)
        .scroll(scroll)

    search.execute.map {
      case Left(failure) => Left(getError(failure))
      case Right(RequestSuccess(status, body, headers, result)) =>
        try {
          result.scrollId match {
            case Some(scrollId) =>
              Right(SearchScrolledBody(
                result.totalHits,
                result.size,
                scrollId,
                getItems(result.hits)
              ))
            case None =>
              Right(SearchPagedBody(
                result.totalHits,
                result.size,
                page.getOrElse(1),
                getItems(result.hits)
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
          Left(getError(failure))
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

private case class Search(
    searchDef: SearchDefinition,
    query: Seq[QueryDefinition],
    size: Option[Int],
    page: Option[Int])
  (implicit elasticsearchConfig: ElasticsearchConfig, client: HttpClient) {

  def storage(storageUuid: String): Search = {
    this.copy(searchDef = search(getHistoricalStorageIndex(storageUuid)))
  }

  def keys(keys: Iterable[String]): Search = {
    if (keys.nonEmpty)
      this.copy(query = query :+ termsQuery("key", keys))
    else
      this
  }

  def operations(operations: Iterable[Operation]): Search = {
    if (operations.nonEmpty)
      this.copy(query = query :+ termsQuery("operation", operations.map(_.toString)))
    else
      this
  }

  def time(start: Option[Long], end: Option[Long]): Search = {
    (start, end) match {
      case (None, None) => this
      case (None, Some(e)) => this.copy(query = query :+ rangeQuery("timestamp").lte(e))
      case (Some(s), None) => this.copy(query = query :+ rangeQuery("timestamp").gte(s))
      case (Some(s), Some(e)) =>
        if (s == e)
          this.copy(query = query :+ termQuery("timestamp", s))
        else
          this.copy(query = query :+ rangeQuery("timestamp").gte(s).lte(e))
    }
  }

  def size(value: Option[Int]): Search = {
    this.copy(size = value)
  }

  def page(value: Option[Int]): Search = {
    this.copy(page = value)
  }

  def scroll(value: Option[Long]): Search = {
    if (value.nonEmpty)
      this.copy(searchDef = searchDef.scroll(value.get + "ms"))
    else
      this
  }

  def sort(sort: Iterable[String]): Search = {
    if (sort.nonEmpty)
      this.copy(searchDef =
        searchDef.sortBy(sort.map { field =>
          if (field.head == '-')
            FieldSortDefinition(field.substring(1), order = SortOrder.DESC)
          else
            FieldSortDefinition(field, order = SortOrder.ASC)
        }))
    else
      this
  }

  def execute: Future[Either[RequestFailure, RequestSuccess[SearchResponse]]] = {
    val pageSize = size.getOrElse(elasticsearchConfig.getScrollPageSize)
    val queryDefinition = query.size match {
      case 0 => searchDef
      case 1 => searchDef.query(query.head)
      case _ => searchDef.query(boolQuery().filter(query))
    }
    if (page.nonEmpty)
      client.execute(queryDefinition.size(pageSize).from(pageSize * (page.get - 1)))
    else
      client.execute(queryDefinition.size(pageSize))
  }
}

private object Search {
  def storage(storageUuid: String)(implicit elasticsearchConfig: ElasticsearchConfig, client: HttpClient): Search = {
    Search(searchDef = search(getHistoricalStorageIndex(storageUuid)), Seq.empty, None, None)
  }
}
