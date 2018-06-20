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
import com.bwsw.cloudstack.storage.kv.entity.History
import com.bwsw.cloudstack.storage.kv.error.{InternalError, StorageError}
import com.bwsw.cloudstack.storage.kv.message.response.GetHistoryResponse
import com.bwsw.cloudstack.storage.kv.message.{Clear, Delete, KvHistory, Operation, Set}
import com.bwsw.cloudstack.storage.kv.processor.ElasticsearchKvProcessor.{ValueField, getIds}
import com.bwsw.cloudstack.storage.kv.util.ElasticsearchUtils
import com.sksamuel.elastic4s.http.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHits
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortDefinition, SortOrder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/** A processor of storage histories stored in Elasticsearch
  *
  * @param client the client to send requests to Elasticsearch
  */
class ElasticsearchHistoryProcessor(client: HttpClient, elasticsearchConfig: ElasticsearchConfig) extends HistoryProcessor {

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
          keys: Iterable[String] = List.empty,
          operations: Iterable[Operation] = List.empty,
          start: Long = 0,
          end: Long = 0,
          sort: Iterable[String] = List.empty,
          page: Int = 1,
          size: Int = elasticsearchConfig.getScrollPageSize,
          scroll: String = elasticsearchConfig.getScrollKeepAlive): Future[Either[StorageError, GetHistoryResponse]] = {

    val searchDef = search(ElasticsearchUtils.getStorageIndex(storageUuid))
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

    val withScroll = withPage.scroll(scroll)
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
        Right(GetHistoryResponse(
          success.result.totalHits,
          success.result.size,
          page,
          success.result.scrollId,
          getItems(success.result.hits)
        ))
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
      case Some(s: String) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }

  protected def getLong(name: String)(implicit fields: Map[String, Any]): Long = {
    fields.get(name) match {
      case Some(s: Long) => s
      case _ => throw new RuntimeException("Invalid result")
    }
  }

  protected def getOperation(string: String): Operation = string match {
    case "set" => Set
    case "delete" => Delete
    case "clear" => Clear
    case _ => throw new RuntimeException("Invalid result")
  }

  protected def getItems(searchHits: SearchHits): List[History] = searchHits.hits.map {
    hit =>
      implicit val fields: Map[String, Any] = hit.sourceAsMap
      History(getString("key"), getString("value"), getLong("timestamp"), getOperation(getString("operation")))
  }.toList

}
