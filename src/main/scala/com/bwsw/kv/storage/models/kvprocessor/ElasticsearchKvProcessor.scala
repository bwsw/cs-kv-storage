package com.bwsw.kv.storage.models.kvprocessor

import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.delete.DeleteResponse
import com.sksamuel.elastic4s.http.get.{GetResponse, MultiGetResponse}
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.ElasticDsl

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class ElasticsearchKvProcessor(client: HttpClient) extends KvProcessor{
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Gets value by given key
    * @param storage target storage UUID
    * @param key key of a value
    * @return the value
    */
  def get(storage: String, key: String): Future[String] = {
    client.execute {
      ElasticDsl.get(key).from(storage / "_doc")
    }.transformWith({
      case Success(either: Either[RequestFailure, RequestSuccess[GetResponse]]) =>
        either match {
          case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
          case Right(success) => Future {
            if (success.isError)
              throw new Exception("Request failed")
            else
              success.result.fields("value").toString
          }
        }
      case Failure(either) => Future { throw either.getCause}
    })
  }

  /** Gets value by given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(storage: String, keys: Iterable[String]): Future[Iterable[(String, Option[String])]] = {
    val gets = keys.map{ ElasticDsl.get(_).from(storage / "_doc") }
    client.execute { multiget(gets) }
      .transformWith({
      case Success(either: Either[RequestFailure, RequestSuccess[MultiGetResponse]]) =>
        either match {
          case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
          case Right(success) => Future {
            if (success.isError)
              throw new Exception("Request failed")
            else
              success.result.docs.map( getResponse =>
                if(getResponse.found)
                  (getResponse.id, Some(getResponse.fields("value").toString))
                else
                  (getResponse.id, None) )
          }
        }
      case Failure(either) => Future { throw either.getCause}
    })
  }

  /** Sets value for given key
    * @param storage target storage UUID
    * @param key key of a value
    * @param value value to set
    * @return true
    */
  def set(storage: String, key: String, value: String): Future[Boolean] = {
    client.execute {
      indexInto(storage / "_doc") id key fields (
        "value" -> value )
    }.transformWith({
      case Success(either: Either[RequestFailure, RequestSuccess[IndexResponse]]) =>
        either match {
          case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
          case Right(success) => Future {
            if (success.isError)
              throw new Exception("Request failed")
            else
              true
          }
        }
      case Failure(either) => Future { throw either.getCause}
    })
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to true if value successfully set and false otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Iterable[(String, Boolean)]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val sets = kvs.map{ case (key, value) => indexInto(storage / "_doc") id key fields (
      "value" -> value ) }
    client.execute { bulk(sets) }
      .transformWith({
        case Success(either: Either[RequestFailure, RequestSuccess[BulkResponse]]) =>
          either match {
            case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
            case Right(success) => Future {
              if (success.isError)
                throw new Exception("Request failed")
              else
                success.result.items.map( bulkResponseItem =>
                  (bulkResponseItem.id, bulkResponseItem.error.isEmpty) )
            }
          }
        case Failure(either) => Future { throw either.getCause}
      })
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return true
    */
  def delete(storage: String, key: String): Future[Boolean] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      deleteById(storage,"_doc", key)
    }.transformWith({
      case Success(either: Either[RequestFailure, RequestSuccess[DeleteResponse]]) =>
        either match {
          case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
          case Right(success) => Future {
            if (success.isError)
              throw new Exception("Request failed")
            else
              true
          }
        }
      case Failure(either) => Future { throw either.getCause}
    })
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed or false otherwise
    */
  def delete(storage: String, keys: Iterable[String]): Future[Iterable[(String, Boolean)]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val deletes = keys.map{ deleteById(storage,"_doc", _) }
    client.execute { bulk(deletes) }
      .transformWith({
        case Success(either: Either[RequestFailure, RequestSuccess[BulkResponse]]) =>
          either match {
            case Left(failure) => Future { throw new Exception(failure.body.getOrElse("Request failed")) }
            case Right(success) => Future {
              if (success.isError)
                throw new Exception("Request failed")
              else
                success.result.items.map( bulkResponseItem =>
                  (bulkResponseItem.id, bulkResponseItem.error.isEmpty) )
            }
          }
        case Failure(either) => Future { throw either.getCause}
      })
  }

  /** Returns a list of existing keys and values
    * @param storage target storage UUID
    * @return Collection of tuples key and value
    */
  def list(storage: String): Future[Iterable[(String, String)]] = {
    Future(Set(("", "")))//TODO: Implementation
  }

  /** Clears storage
    * @param storage target storage UUID
    * @return true if clearing succeed and false otherwise
    */
  def clear(storage: String): Future[Boolean] = {
    Future(true)//TODO: Implementation
  }
}
