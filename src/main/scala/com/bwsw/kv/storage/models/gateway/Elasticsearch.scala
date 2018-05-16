package com.bwsw.kv.storage.models.gateway

import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import akka.actor.Actor
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

class Elasticsearch extends Gateway{
  val elasticClient: RestHighLevelClient = new RestHighLevelClient(
    RestClient.builder(
      new HttpHost("localhost", 9200, "http"),
      new HttpHost("localhost", 9201, "http")))

  /** Gets value by given key
    * @param storage target storage UUID
    * @param key key of a value
    * @return Some(String) if value exists and None otherwise
    */
  def get(storage: String, key: String)(implicit ctx: ExecutionContext): Future[Option[String]] = {
    val prom = Promise[Option[String]]()
    val searchListener: ActionListener[SearchResponse] =  new ActionListener[SearchResponse]() {
      @Override
      def onResponse(searchResponse: SearchResponse) {

        prom.complete(Try(Some("")))//TODO: Response unpack
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val searchRequest = new SearchRequest(storage, "doc")
    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    sourceBuilder.query(QueryBuilders.termQuery("key", key))
    searchRequest.source(sourceBuilder)
    elasticClient.searchAsync(searchRequest, searchListener)
    prom.future
  }

  /** Gets value by given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key and Some(String) if value exists and None otherwise
    */
  def get(storage: String, keys: Iterable[String])(implicit ctx: ExecutionContext): Future[Iterable[(String, Option[String])]] = {
    val prom = Promise[Iterable[(String, Option[String])]]()
    val searchListener: ActionListener[SearchResponse] =  new ActionListener[SearchResponse]() {
      @Override
      def onResponse(searchResponse: SearchResponse) {

        prom.complete(Try(Set(("", None))))//TODO: Response unpack
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val searchRequest = new SearchRequest(storage, "doc")
    val sourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    sourceBuilder.query(QueryBuilders.termQuery("key", keys))
    searchRequest.source(sourceBuilder)
    elasticClient.searchAsync(searchRequest, searchListener)
    prom.future
  }

  /** Sets value for given key
    * @param storage target storage UUID
    * @param key key of a value
    * @param value value to set
    * @return Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, key: String, value: String)(implicit ctx: ExecutionContext): Future[Option[String]] = {
    val prom = Promise[Option[String]]()
    val indexListener: ActionListener[IndexResponse] =  new ActionListener[IndexResponse]() {
      @Override
      def onResponse(indexResponse: IndexResponse) {
        prom.complete(Try(Some("")))//TODO: Response unpack
      }
      @Override
      def onFailure(e: Exception) {
        prom.failure(e)
      }
    }
    val indexRequest = new IndexRequest(storage, "doc")
      .source(key, value)
      .version(1)
    elasticClient.indexAsync(indexRequest, indexListener)
    prom.future
  }

  /** Sets values by given keys
    * @param storage target storage UUID
    * @param kvs Collection of tuples key and value to set
    * @return Collection of tuples key to Some(String) if value successfully set and None otherwise
    */
  def set(storage: String, kvs: Iterable[(String, String)])(implicit ctx: ExecutionContext): Future[Iterable[(String, Option[String])]] = {
    Future(Set(("", None)))//TODO: Implementation
  }

  /** Deletes value of given key
    * @param storage target storage UUID
    * @param key key of value to delete
    * @return true if deletion succeed and false otherwise
    */
  def delete(storage: String, key: String)(implicit ctx: ExecutionContext): Future[Boolean] = {
    Future(true)//TODO: Implementation
  }

  /** Deletes values of given keys
    * @param storage target storage UUID
    * @param keys Collection of keys
    * @return Collection of tuples key to true if deletion succeed and false otherwise
    */
  def delete(storage: String, keys: Iterable[String])(implicit ctx: ExecutionContext): Future[Iterable[(String, Boolean)]] = {
    Future(Set(("", true)))//TODO: Implementation
  }

  /** Returns a list of existing keys and values
    * @param storage target storage UUID
    * @return Collection of tuples key and value
    */
  def list(storage: String)(implicit ctx: ExecutionContext): Future[Iterable[(String, String)]] = {
    Future(Set(("", "")))//TODO: Implementation
  }

  /** Clears storage
    * @param storage target storage UUID
    * @return true if clearing succeed and false otherwise
    */
  def clear(storage: String)(implicit ctx: ExecutionContext): Future[Boolean] = {
    Future(true)//TODO: Implementation
  }

  /** Closes connection to Elasticsearch
    */
  def closeClient(): Unit = {
    elasticClient.close()
  }
}
