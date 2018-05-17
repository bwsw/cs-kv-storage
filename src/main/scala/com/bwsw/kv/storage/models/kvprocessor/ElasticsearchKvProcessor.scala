package com.bwsw.kv.storage.models.kvprocessor

import collection.JavaConverters._
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.get._
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class ElasticsearchKvProcessor(client: RestHighLevelClient) extends KvProcessor{
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Gets value by given key
    * @param storage target storage UUID
    * @param key key of a value
    * @return the value
    */
  def get(storage: String, key: String): Future[String] = {
    val prom = Promise[String]()
    val getListener: ActionListener[GetResponse] =  new ActionListener[GetResponse]() {
      @Override
      def onResponse(getResponse: GetResponse) {
        if (getResponse.isExists)
          prom.complete(Try(getResponse.getField("value").getValue))
        else
          prom.failure(new NullPointerException)
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val getRequest = new GetRequest(storage, "_doc", key)
    client.getAsync(getRequest, getListener)
    prom.future
  }

  /** Gets value by given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(storage: String, keys: Iterable[String]): Future[Iterable[(String, Option[String])]] = {
    val prom = Promise[Iterable[(String, Option[String])]]()
    val multiGetListener: ActionListener[MultiGetResponse] =  new ActionListener[MultiGetResponse]() {
      @Override
      def onResponse(multiGetResponse: MultiGetResponse) {
        prom.complete(Try(multiGetResponse.getResponses.map( response =>
          if(response.isFailed) {
            (response.getResponse.getId, None)
          }
          else {
            val aGetResponse = response.getResponse
            (aGetResponse.getId, aGetResponse.getField("value").getValue)
          })))
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val multiGetRequest = new MultiGetRequest()
    keys.foreach(key => multiGetRequest.add(storage, "_doc", key))
    client.multiGetAsync(multiGetRequest, multiGetListener)
    prom.future
  }

  /** Sets value for given key
    * @param storage target storage UUID
    * @param key key of a value
    * @param value value to set
    * @return Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, key: String, value: String): Future[String] = {
    val prom = Promise[String]()
    val indexListener: ActionListener[IndexResponse] =  new ActionListener[IndexResponse]() {
      @Override
      def onResponse(indexResponse: IndexResponse) {
        prom.complete(Try(""))//TODO: Response unpack
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val indexRequest = new IndexRequest(storage, "doc")
      .source(key, value)
      .version(1)
    client.indexAsync(indexRequest, indexListener)
    prom.future
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Iterable[(String, String)]] = {
    Future(Set(("", "")))//TODO: Implementation
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return true if deletion succeed and false otherwise
    */
  def delete(storage: String, key: String): Future[Boolean] = {
    Future(true)//TODO: Implementation
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed and false otherwise
    */
  def delete(storage: String, keys: Iterable[String]): Future[Iterable[(String, Boolean)]] = {
    Future(Set(("", true)))//TODO: Implementation
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
