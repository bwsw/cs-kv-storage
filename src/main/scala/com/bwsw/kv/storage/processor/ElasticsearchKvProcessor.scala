package com.bwsw.kv.storage.processor

import com.bwsw.kv.storage.error._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.ElasticDsl
import com.bwsw.kv.storage.app.Configuration

import scala.concurrent.Future

object ElasticsearchKvProcessor {
  private val basicType = "_doc"
}

class ElasticsearchKvProcessor(client: HttpClient, configuration: Configuration) extends KvProcessor {
  import scala.concurrent.ExecutionContext.Implicits.global
  import ElasticsearchKvProcessor._

  /** Gets value by given key
    * @param storage targeted storage UUID
    * @param key key of a value
    * @return Future of the value
    *         or Future that fails with RuntimeException when request fails
    */
  def get(storage: String, key: String): Future[Either[StorageError, String]] = {
    client.execute {
      ElasticDsl.get(key).from(("storage-" + storage) / basicType) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) =>
          if(success.result.found)
            Right(success.result.source("value").toString)
          else
            Left(NotFoundError()) }
  }

  /** Gets value by given keys
    * @param storage targeted storage UUID
    * @param keys Collection of keys
    * @return Future of Collection of tuples key and Some(String) if value exists and None otherwise
    *         or Future that fails with RuntimeException when request fails
    */
  def get(storage: String, keys: Iterable[String]):
    Future[Either[StorageError, Map[String, Option[String]]]] = {
    val gets = keys.map{ ElasticDsl.get(_).from(("storage-" + storage) / basicType) }
    client.execute { multiget(gets) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) =>
          Right(success.result.docs.map(getResponse =>
            if (getResponse.found)
              (getResponse.id, Some(getResponse.source("value").toString))
            else
              (getResponse.id, None)).toMap) }
  }

  /** Sets value for given key
    * @param storage targeted storage UUID
    * @param key key of a value
    * @param value value to set
    * @return Future of true
    *         or Future that fails with RuntimeException when request fails
    */
  def set(storage: String, key: String, value: String): Future[Either[StorageError, Unit]] = {
    client.execute {
      indexInto(("storage-" + storage) / basicType) id key fields (
        "value" -> value ) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) => Right(Unit) }
  }

  /** Sets values by given keys
    * @param storage targeted storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Future of Collection of tuples key to true if value successfully set and false otherwise
    *         or Future that fails with RuntimeException when request fails
    */
  def set(storage: String, kvs: Iterable[(String, String)]):
    Future[Either[StorageError, Map[String, Boolean]]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val sets = kvs.map { case (key, value) =>
      indexInto(("storage-" + storage) / basicType) id key fields(
        "value" -> value) }
    client.execute { bulk(sets) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) =>
          Right(success.result.items.map(bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap) }
  }

  /** Deletes value of given key
    * @param storage targeted storage UUID
    * @param key key of value to delete
    * @return Future of true
    *         or Future that fails with RuntimeException when request fails
    */
  def delete(storage: String, key: String): Future[Either[StorageError, Unit]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      deleteById("storage-" + storage, basicType, key) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) => Right(Unit) }
  }

  /** Deletes values of given keys
    * @param storage targeted storage UUID
    * @param keys Collection of keys
    * @return Future of Collection of tuples key to true if deletion succeed or false otherwise
    *         or Future that fails with RuntimeException when request fails
    */
  def delete(storage: String, keys: Iterable[String]):
    Future[Either[StorageError, Map[String, Boolean]]] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val deletes = keys.map{ deleteById("storage-" + storage, basicType, _) }
    client.execute { bulk(deletes) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) =>
          Right(success.result.items.map(bulkResponseItem =>
            (bulkResponseItem.id, bulkResponseItem.error.isEmpty)).toMap) }
  }

  /** Returns a list of existing keys and values
    * @param storage targeted storage UUID
    * @return Future of Collection of tuples key and value
    *         or Future that fails with RuntimeException when request fails
    */
  def list(storage: String): Future[Either[StorageError, List[String]]] = {
    client.execute { search("storage-" + storage).size(configuration.getSearchPageSize)
    .scroll(configuration.getSearchScrollKeepAlive)}
      .flatMap {
        case Left(failure) => Future(Left(InternalError(failure)))
        case Right(success) =>
          if(success.result.scrollId.nonEmpty) {
            scrollAll(success.result.scrollId.get, success.result.hits.hits.map(hit =>
              hit.id).toList)
          }
          else {
            Future(Right(success.result.hits.hits.map(hit => hit.id).toList)) }
          }
  }

  /** Recursively gets contents of all pages of the search query
    * @param scrollId Scroll Id
    * @param currentList List of currently hit keys
    * @return Future of list containing keys hit by the query
    */
  private def scrollAll(scrollId: String, currentList: List[String]): Future[Either[StorageError, List[String]]]= {
    client.execute(searchScroll(scrollId))
      .flatMap {
        case Left(failure) => Future(Left(InternalError(failure)))
        case Right(success) =>
          if(success.result.hits.hits.length == 0){
            client.execute { clearScroll(scrollId)}
            Future(Right(currentList))
          }
          else {
            scrollAll(scrollId, currentList ++ success.result.hits.hits.map(hit => hit.id).toList) } }
  }
  /** Clears storage
    * @param storage targeted storage UUID
    * @return Future of true
    *         or Future that fails with RuntimeException when request fails
    */
  def clear(storage: String): Future[Either[StorageError, Unit]] = {
    client.execute {
      deleteByQuery("storage-" + storage, basicType, matchAllQuery)
        .proceedOnConflicts(true) }
      .map {
        case Left(failure) => Left(InternalError(failure))
        case Right(success) =>
          if(success.result.versionConflicts > 0)
            Left(ConflictError())
          else
            Right(Unit) }
  }
}
