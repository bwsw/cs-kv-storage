package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.app.Configuration
import com.bwsw.kv.storage.error._
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.SearchHits
import com.sksamuel.elastic4s.http.{ElasticDsl, HttpClient, RequestFailure}

import scala.concurrent.Future

/** A processor for Elasticsearch key/value storages.
  *
  * @param client        the client to send requests to Elasticsearch
  * @param configuration the configuration
  */
class ElasticsearchKvProcessor(client: HttpClient, configuration: Configuration) extends KvProcessor {

  import ElasticsearchKvProcessor._

  import scala.concurrent.ExecutionContext.Implicits.global

  def get(storage: String, key: String): Future[Either[StorageError, String]] = {
    client.execute {
      ElasticDsl.get(key).from(getIndex(storage) / Type)
    }.map {
      case Left(failure) => Left(getError(failure))
      case Right(success) =>
        if (success.result.found)
          getValue(success.result.source)
        else
          Left(NotFoundError())
    }
  }

  def get(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Option[String]]]] = {
    val gets = keys.map {
      ElasticDsl.get(_).from(getIndex(storage) / Type)
    }
    client.execute {
      multiget(gets)
    }.map {
      case Left(failure) => Left(getError(failure))
      case Right(success) => getValues(success.result.docs, keys.map(e => e -> None).toMap)
    }
  }

  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]] = {
    if (isKvValid(key, value))
      client.execute {
        indexInto(getIndex(storage) / Type) id key fields (ValueField -> value)
      }.map {
        case Left(failure) => Left(getError(failure))
        case Right(success) => Right(Unit)
      }
    else
      Future(Left(BadRequestError()))
  }

  def set(storage: String, kvs: Map[String, String]): Future[Either[StorageError, Map[String, Boolean]]] = {
    val splitKvs = kvs.partition { kv => isKvValid(kv._1, kv._2) }
    val sets = splitKvs._1.map { case (key, value) => indexInto(getIndex(storage) / Type) id key fields (ValueField -> value) }
    val bad = splitKvs._2.map { case (key, _) => (key, false) }
    if (sets.nonEmpty) {
      client.execute {
        bulk(sets)
      }.map {
        case Left(failure) => Left(getError(failure))
        case Right(success) =>
          Right(success.result.items.map(bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap ++ bad)
      }
    } else
      Future(Right(bad))
  }

  def delete(storage: String, key: String): Future[Either[StorageError, Unit]] = {
    client.execute {
      deleteById(getIndex(storage), Type, key)
    }.map {
      case Left(failure) => Left(getError(failure))
      case Right(success) => Right(Unit)
    }
  }

  def delete(storage: String, keys: Iterable[String]): Future[Either[StorageError, Map[String, Boolean]]] = {
    val deletes = keys.map {
      deleteById(getIndex(storage), Type, _)
    }
    client.execute {
      bulk(deletes)
    }.map {
      case Left(failure) => Left(getError(failure))
      case Right(success) =>
        Right(success.result.items.map(bulkResponseItem =>
          (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap)
    }
  }

  def list(storage: String): Future[Either[StorageError, List[String]]] = {
    val keepAlive = configuration.getSearchScrollKeepAlive
    client.execute {
      search(getIndex(storage)).size(configuration.getSearchPageSize)
        .scroll(keepAlive)
    }.flatMap {
      case Left(failure) => Future(Left(getError(failure)))
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
      deleteByQuery(getIndex(storage), Type, matchAllQuery)
        .proceedOnConflicts(true)
    }
      .map {
        case Left(failure) => Left(getError(failure))
        case Right(success) =>
          if (success.result.versionConflicts > 0)
            Left(ConflictError())
          else
            Right(Unit)
      }
  }

  private def scrollAll(scrollId: String, results: List[String], keepAlive: String): Future[Either[StorageError, List[String]]] = {
    client.execute(searchScroll(scrollId).keepAlive(keepAlive))
      .flatMap {
        case Left(failure) => Future(Left(getError(failure)))
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

  private def isKeyValid(key: String): Boolean = {
    key != null && !key.isEmpty && (configuration.getMaxKeyLength == -1 || key.length <= configuration.getMaxKeyLength)
  }

  private def isValueValid(value: String): Boolean = {
    value == null || configuration.getMaxValueLength == -1 || value.length <= configuration.getMaxValueLength
  }

  private def isKvValid(key: String, value: String): Boolean = isKeyValid(key) && isValueValid(value)
}

/** ElasticsearchKvProcessor companion object. **/
object ElasticsearchKvProcessor {
  private val Type = "_doc"
  private val ValueField = "value"

  private def getIndex(storage: String): String = "storage-" + storage

  private def getError(requestFailure: RequestFailure): InternalError = {
    if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }

  private def getValue(fields: Map[String, Any]): Either[StorageError, String] = {
    fields.get(ValueField) match {
      case Some(null) => Right(null)
      case Some(s: String) => Right(s)
      case _ => Left(InternalError("Invalid result"))
    }
  }

  private def getValues(responses: Iterable[GetResponse], results: Map[String, Option[String]]): Either[StorageError, Map[String, Option[String]]] = {
    responses match {
      case Nil => Right(results)
      case r :: tail =>
        if (r.found) {
          val result = getValue(r.source)
          result match {
            case Left(error) => Left(error)
            case Right(value) => getValues(tail, results + (r.id -> Some(value)))
          }
        }
        else
          getValues(tail, results + (r.id -> None))
    }
  }

  private def getIds(searchHits: SearchHits): List[String] = searchHits.hits.map(hit => hit.id).toList
}
