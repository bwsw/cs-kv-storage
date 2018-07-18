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

import java.nio.charset.StandardCharsets

import com.bwsw.cloudstack.storage.kv.configuration.ElasticsearchConfig
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, ConflictError, InternalError, NotFoundError, StorageError}
import com.bwsw.cloudstack.storage.kv.util.elasticsearch._
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.SearchHits
import com.sksamuel.elastic4s.http.{ElasticDsl, HttpClient}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/** A processor for Elasticsearch key/value storages.
  *
  * @param client the client to send requests to Elasticsearch
  * @param conf   the configuration
  */
class ElasticsearchKvProcessor(client: HttpClient, conf: ElasticsearchConfig) extends KvProcessor {

  import ElasticsearchKvProcessor._

  import scala.concurrent.ExecutionContext.Implicits.global

  def get(storage: String, key: String): Future[Either[StorageError, String]] = {
    client.execute {
      ElasticDsl.get(key).from(getStorageIndex(storage) / DocumentType)
    }.map {
      case Left(failure) =>
        logger.error("Elasticsearch get request failure: {}", failure.error)
        Left(getError(failure))
      case Right(success) =>
        if (success.result.found)
          getValue(success.result.source)
        else
          Left(NotFoundError())
    }
  }

  def get(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Option[String]]]] = {
    val gets = keys.map {
      ElasticDsl.get(_).from(getStorageIndex(storage) / DocumentType)
    }
    client.execute {
      multiget(gets)
    }.map {
      case Left(failure) =>
        logger.error("Elasticsearch multiget request failure: {}", failure.error)
        Left(getError(failure))
      case Right(success) => getValues(success.result.docs, keys.map(e => e -> None).toMap)
    }
  }

  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]] = {
    if (isKvValid(key, value))
      client.execute {
        indexInto(getStorageIndex(storage) / DocumentType) id key fields (StorageFields.Value -> value)
      }.map {
        case Left(failure) =>
          logger.error("Elasticsearch index request failure: {}", failure.error)
          Left(getError(failure))
        case Right(_) => Right(())
      }
    else
      Future(Left(BadRequestError()))
  }

  def set(storage: String, kvs: Map[String, String]): Future[Either[StorageError, Map[String, Boolean]]] = {
    val splitKvs = kvs.partition { kv => isKvValid(kv._1, kv._2) }
    val sets = splitKvs._1.map { case (key, value) =>
      indexInto(getStorageIndex(storage) / DocumentType) id key fields (StorageFields.Value -> value)
    }
    val bad = splitKvs._2.map { case (key, _) => (key, false) }
    if (sets.nonEmpty) {
      client.execute {
        bulk(sets)
      }.map {
        case Left(failure) =>
          logger.error("Elasticsearch bulk index request failure: {}", failure.error)
          Left(getError(failure))
        case Right(success) =>
          Right(success.result.items.map(bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap ++ bad)
      }
    } else
      Future(Right(bad))
  }

  def delete(storage: String, key: String): Future[Either[StorageError, Unit]] = {
    client.execute {
      deleteById(getStorageIndex(storage), DocumentType, key)
    }.map {
      case Left(failure) =>
        logger.error("Elasticsearch delete by id request failure: {}", failure.error)
        Left(getError(failure))
      case Right(_) => Right(())
    }
  }

  def delete(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Boolean]]] = {
    val deletes = keys.map {
      deleteById(getStorageIndex(storage), DocumentType, _)
    }
    client.execute {
      bulk(deletes)
    }.map {
      case Left(failure) =>
        logger.error("Elasticsearch bulk delete by id request failure: {}", failure.error)
        Left(getError(failure))
      case Right(success) =>
        Right(success.result.items.map(bulkResponseItem => (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap)
    }
  }

  def list(storage: String): Future[Either[StorageError, List[String]]] = {
    val keepAlive = conf.getScrollKeepAlive
    client.execute {
      search(getStorageIndex(storage)).size(conf.getScrollPageSize)
        .scroll(keepAlive)
    }.flatMap {
      case Left(failure) =>
        logger.error("Elasticsearch search request failure: {}", failure.error)
        Future(Left(getError(failure)))
      case Right(success) =>
        if (success.result.scrollId.nonEmpty) {
          scrollAll(success.result.scrollId.get, getIds(success.result.hits), keepAlive)
        } else {
          Future(Right(getIds(success.result.hits)))
        }
    }
  }

  def clear(storage: String): Future[Either[StorageError, Unit]] = {
    client.execute {
      deleteByQuery(getStorageIndex(storage), DocumentType, matchAllQuery)
        .proceedOnConflicts(true)
    }
      .map {
        case Left(failure) =>
          logger.error("Elasticsearch delete by query request failure: {}", failure.error)
          Left(getError(failure))
        case Right(success) =>
          if (success.result.versionConflicts > 0)
            Left(ConflictError())
          else
            Right(())
      }
  }

  private def scrollAll(
      scrollId: String,
      results: List[String],
      keepAlive: String): Future[Either[StorageError, List[String]]] = {
    client.execute(searchScroll(scrollId).keepAlive(keepAlive))
      .flatMap {
        case Left(failure) =>
          logger.error("Elasticsearch scroll request failure: {}", failure.error)
          Future(Left(getError(failure)))
        case Right(success) =>
          if (success.result.hits.hits.length == 0) {
            client.execute {
              clearScroll(success.result.scrollId.get)
            }
            Future(Right(results))
          } else {
            scrollAll(success.result.scrollId.get, results ++ getIds(success.result.hits), keepAlive)
          }
      }
  }

  private def isKeyValid(key: String): Boolean = key != null && !key.isEmpty && getLength(key) <= conf.getMaxKeyLength

  private def isValueValid(value: String): Boolean = {
    value == null || conf.getMaxValueLength == -1 || getLength(value) <= conf.getMaxValueLength
  }

  private def isKvValid(key: String, value: String): Boolean = isKeyValid(key) && isValueValid(value)
}

/** ElasticsearchKvProcessor companion object. **/
object ElasticsearchKvProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  private def getValue(fields: Map[String, Any]): Either[StorageError, String] = {
    fields.get(StorageFields.Value) match {
      case Some(null) => Right(null)
      case Some(s: String) => Right(s)
      case _ => Left(InternalError("Invalid result"))
    }
  }

  private def getValues(
      responses: Iterable[GetResponse],
      results: Map[String, Option[String]]): Either[StorageError, Map[String, Option[String]]] = {
    responses match {
      case Nil => Right(results)
      case r :: tail =>
        if (r.found) {
          val result = getValue(r.source)
          result match {
            case Left(error) =>
              logger.error("Field {} is not specified in the response: {}", StorageFields.Value, r: Any)
              Left(error)
            case Right(value) => getValues(tail, results + (r.id -> Some(value)))
          }
        }
        else
          getValues(tail, results + (r.id -> None))
    }
  }

  private def getIds(searchHits: SearchHits): List[String] = searchHits.hits.map(hit => hit.id).toList

  private def getLength(value: String): Int = value.getBytes(StandardCharsets.UTF_8).length
}
