package com.bwsw.kv.storage.models.kvprocessor

import collection.JavaConverters._
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
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
        prom.complete(Try(multiGetResponse.getResponses.map{ response =>
          if(response.isFailed) {
            (response.getResponse().getId, None)
          }
          else {
            val aGetResponse = response.getResponse()
            (aGetResponse.getId, Some(aGetResponse.getField("value").getValue))
          } }))
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
    * @return true
    */
  def set(storage: String, key: String, value: String): Future[Boolean] = {
    val prom = Promise[Boolean]()
    val indexListener: ActionListener[IndexResponse] =  new ActionListener[IndexResponse]() {
      @Override
      def onResponse(indexResponse: IndexResponse) {
        prom.complete(Try(true))
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val indexRequest = new IndexRequest(storage, "_doc", key)
      .source("value", value)
    client.indexAsync(indexRequest, indexListener)
    prom.future
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to true if value successfully set and false otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)]): Future[Iterable[(String, Boolean)]] = {
    val prom = Promise[Iterable[(String, Boolean)]]()
    val bulkListener: ActionListener[BulkResponse] =  new ActionListener[BulkResponse]() {
      @Override
      def onResponse(bulkResponse: BulkResponse) {
        prom.complete(Try(bulkResponse.getItems.map{ response =>
          val indexResponse: IndexResponse = response.getResponse()
          (indexResponse.getId, !response.isFailed) }))
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val bulkRequest = new BulkRequest
    kvs.foreach{ case (key, value) => bulkRequest.add(new IndexRequest(storage, "_doc", key).source("value", value)) }
    client.bulkAsync(bulkRequest, bulkListener)
    prom.future
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return true
    */
  def delete(storage: String, key: String): Future[Boolean] = {
    val prom = Promise[Boolean]()
    val deleteListener: ActionListener[DeleteResponse] =  new ActionListener[DeleteResponse]() {
      @Override
      def onResponse(deleteResponse: DeleteResponse) {
        prom.complete(Try(true))
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val deleteRequest = new DeleteRequest(storage, "_doc", key)
    client.deleteAsync(deleteRequest, deleteListener)
    prom.future
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed or false otherwise
    */
  def delete(storage: String, keys: Iterable[String]): Future[Iterable[(String, Boolean)]] = {
    val prom = Promise[Iterable[(String, Boolean)]]()
    val bulkListener: ActionListener[BulkResponse] =  new ActionListener[BulkResponse]() {
      @Override
      def onResponse(bulkResponse: BulkResponse) {
        prom.complete(Try(bulkResponse.getItems.map{ response =>
          val deleteResponse: DeleteResponse = response.getResponse()
          (deleteResponse.getId, !response.isFailed) }))
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val bulkRequest = new BulkRequest
    keys.foreach{ key => bulkRequest.add(new DeleteRequest(storage, "_doc", key)) }
    client.bulkAsync(bulkRequest, bulkListener)
    prom.future
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
