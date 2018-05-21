package com.bwsw.kv.storage.models.kvprocessor

import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.delete.{DeleteByQueryResponse, DeleteResponse}
import com.sksamuel.elastic4s.http.get.{GetResponse, MultiGetResponse}
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.ElasticDsl
import com.sksamuel.elastic4s.http.search.SearchResponse

import scala.concurrent.Future
import scala.util.{Failure, Success}

class ElasticsearchKvProcessor(client: HttpClient) extends KvProcessor {
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Gets value by given key
    * @param storage target storage UUID
    * @param key key of a value
    * @return Future of the value
    */
  def get(storage: String, key: String): Future[String] = {
    client.execute {
      ElasticDsl.get(key).from(storage / "_doc") }
      .map{
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) => success.result.fields("value").toString }
  }

  /** Gets value by given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Future of Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(storage: String, keys: Iterable[String]): Future[Iterable[(String, Option[String])]] = {
    val gets = keys.map{ ElasticDsl.get(_).from(storage / "_doc") }
    client.execute { multiget(gets) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) =>
          success.result.docs.map(getResponse =>
            if (getResponse.found)
              (getResponse.id, Some(getResponse.fields("value").toString))
            else
              (getResponse.id, None)) }
  }

  /** Sets value for given key
    * @param storage target storage UUID
    * @param key key of a value
    * @param value value to set
    * @return Future of true
    */
  def set(storage: String, key: String, value: String): Future[Boolean] = {
    client.execute {
      indexInto(storage / "_doc") id key fields (
        "value" -> value ) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) => true }
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Future of Collection of tuples key to true if value successfully set and false otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Iterable[(String, Boolean)]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val sets = kvs.map{ case (key, value) => indexInto(storage / "_doc") id key fields (
      "value" -> value ) }
    client.execute { bulk(sets) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) =>
          success.result.items.map(bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty)) }
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return Future of true
    */
  def delete(storage: String, key: String): Future[Boolean] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      deleteById(storage,"_doc", key) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) => true }
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Future of Collection of tuples key to true if deletion succeed or false otherwise
    */
  def delete(storage: String, keys: Iterable[String]): Future[Iterable[(String, Boolean)]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val deletes = keys.map{ deleteById(storage,"_doc", _) }
    client.execute { bulk(deletes) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) =>
          success.result.items.map( bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty) ) }
  }

  /** Returns a list of existing keys and values
    * @param storage target storage UUID
    * @return Future of Collection of tuples key and value
    */
  def list(storage: String): Future[Iterable[(String, String)]] = {
    client.execute { search(storage) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) =>
          success.result.hits.hits.map( hit =>
            (hit.id, hit.fields("value").toString) ) }
  }

  /** Clears storage
    * @param storage target storage UUID
    * @return Future of true
    */
  def clear(storage: String): Future[Boolean] = {
    client.execute {
      deleteByQuery(storage,"_doc", matchAllQuery).proceedOnConflicts(true) }
      .map {
        case Left(failure) => throw new RuntimeException(failure.error.reason)
        case Right(success) => true }
  }
}
