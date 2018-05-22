package com.bwsw.kv.storage.kvprocessor

import com.bwsw.kv.storage.models.kvprocessor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.delete.{DeleteByIdDefinition, DeleteByQueryDefinition}
import com.sksamuel.elastic4s.get.{GetDefinition, MultiGetDefinition}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.{GetResponse, MultiGetResponse}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.{BulkResponse, BulkResponseItem, BulkResponseItems}
import com.sksamuel.elastic4s.http.delete.{DeleteByQueryResponse, DeleteResponse}
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits, SearchResponse}
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.searches.SearchDefinition
import org.scalamock.handlers.CallHandler3
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpec
import org.scalatest.RecoverMethods._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchKvProcessorUnitTest extends FunSpec with MockFactory {
  private def getRequestSuccessFuture[T](response: T): Future[Right[RequestFailure, RequestSuccess[T]]] = {
    Future(Right(RequestSuccess(200, Option.empty, Map.empty, response)))
  }
  private def getRequestFailureFuture[T]: Future[Left[RequestFailure, RequestSuccess[T]]] = {
    Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
  }
  private def getGetRequest(client: HttpClient) = {
    (client.execute[GetDefinition, GetResponse] (_:GetDefinition)(_:HttpExecutable[GetDefinition, GetResponse], _ : ExecutionContext))
      .expects(ElasticDsl.get("someKey").from("someStorage" / "_doc"), GetHttpExecutable, *)
  }
  private def getMultiGetRequest(client: HttpClient) = {
    val gets = Set(
      ElasticDsl.get("key1").from("someStorage" / "_doc"),
      ElasticDsl.get("key2").from("someStorage" / "_doc"),
      ElasticDsl.get("key3").from("someStorage" / "_doc"))
    (client.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
      .expects(multiget(gets), MultiGetHttpExecutable, *)
  }
  private def getIndexRequest(client: HttpClient) = {
    (client.execute[IndexDefinition, IndexResponse] (_:IndexDefinition)(_:HttpExecutable[IndexDefinition, IndexResponse], _ : ExecutionContext))
      .expects(indexInto("someStorage" / "_doc") id "someKey" fields ("value" -> "someValue" ), IndexHttpExecutable, *)
  }
  private def getBulkIndexRequest(client: HttpClient) = {
    val sets = Set(
      indexInto("someStorage" / "_doc") id "key1" fields ("value" -> "value1" ),
      indexInto("someStorage" / "_doc") id "key2" fields ("value" -> "value2" ),
      indexInto("someStorage" / "_doc") id "key3" fields ("value" -> "value3" )
    )

    (client.execute[BulkDefinition, BulkResponse] (_:BulkDefinition)(_:HttpExecutable[BulkDefinition, BulkResponse], _ : ExecutionContext))
      .expects(ElasticDsl.bulk(sets), BulkExecutable, *)
  }
  private def getDeleteRequest(client: HttpClient) = {
    (client.execute[DeleteByIdDefinition, DeleteResponse] (_:DeleteByIdDefinition) (_:HttpExecutable[DeleteByIdDefinition, DeleteResponse], _ : ExecutionContext))
      .expects(deleteById("someStorage","_doc", "someKey"), DeleteByIdExecutable, *)
  }
  private def getBulkDeleteRequest(client: HttpClient) = {
    val deletes = Set(
      deleteById("someStorage", "_doc", "key1"),
      deleteById("someStorage", "_doc", "key2"),
      deleteById("someStorage", "_doc", "key3")
    )

    (client.execute[BulkDefinition, BulkResponse] (_:BulkDefinition)(_:HttpExecutable[BulkDefinition, BulkResponse], _ : ExecutionContext))
      .expects(ElasticDsl.bulk(deletes), BulkExecutable, *)

  }
  private def getSearchRequest(client: HttpClient) = {
    (client.execute[SearchDefinition, SearchResponse] (_:SearchDefinition)(_:HttpExecutable[SearchDefinition, SearchResponse], _ : ExecutionContext))
      .expects(ElasticDsl.search("someStorage"), SearchHttpExecutable, *)
  }
  private def getDeleteByQueryRequest(client: HttpClient) = {
    (client.execute[DeleteByQueryDefinition, DeleteByQueryResponse] (_:DeleteByQueryDefinition)(_:HttpExecutable[DeleteByQueryDefinition, DeleteByQueryResponse], _ : ExecutionContext))
      .expects(deleteByQuery("someStorage","_doc", matchAllQuery).proceedOnConflicts(true) , DeleteByQueryExecutable, *)
  }

  describe("A ElasticsearchKvProcessor") {
    val fakeClient = mock[HttpClient]
    val elasticsearchKvProcessor = new ElasticsearchKvProcessor(fakeClient)

    //get(storage, key) test start
    it("should get existing value by key from Elasticsearch") {
      val getResponse = GetResponse("someKey", "someStorage", "_doc", 1, found = true, Map("value" -> "someValue"), Map("value" -> "someValue"))
      getGetRequest(fakeClient).returning(getRequestSuccessFuture(getResponse))

      elasticsearchKvProcessor.get("someStorage", "someKey").map { value => assert(value == "someValue") }
    }
    it("get(key) should fail if execute method fails") {
      getGetRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.get("someStorage", "someKey")
      }
    }
    it("get(key) future should fail if request fails") {
      getGetRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.get("someStorage", "someKey")
      }
    }
    //get(storage, key) test end

    //get(storage, keys) test start
    it("should get multiple existing values by keys from Elasticsearch") {
      val multiGetResponse = MultiGetResponse(Seq(
        GetResponse("key1", "someStorage", "_doc", 1, found = true, Map("value" -> "value1"), Map("value" -> "value1")),
        GetResponse("key2", "someStorage", "_doc", 1, found = true, Map("value" -> "value2"), Map("value" -> "value2")),
        GetResponse("key3", "someStorage", "_doc", 1, found = true, Map("value" -> "value3"), Map("value" -> "value3"))))
      getMultiGetRequest(fakeClient).returning(getRequestSuccessFuture(multiGetResponse))

      elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
        .map { values => assert(values == Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")) }
    }
    it("get(keys) should fail if execute method fails") {
      getMultiGetRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
      }
    }
    it("get(keys) future should fail if request fails") {
      getMultiGetRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
      }
    }
    //get(storage, keys) test end

    //set(storage, key, value) test start
    it("should set value by key into Elasticsearch") {
      val indexResponse = IndexResponse("someKey", "someStorage", "_doc", 1, "someResult", forcedRefresh = true, Shards(1, 1, 1))
      getIndexRequest(fakeClient).returning(getRequestSuccessFuture(indexResponse))

      elasticsearchKvProcessor.set("someStorage", "someKey", "someValue")
        .map { value => assert(value) }
    }
    it("set(key, value) should fail if execute method fails") {
      getIndexRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.set("someStorage", "someKey", "someValue")
      }
    }
    it("set(key, value) future should fail if request fails") {
      getIndexRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.set("someStorage", "someKey", "someValue")
      }
    }
    //set(storage, key, value) test end

    //set(storage, kvs) test start
    it("should set multiple values by keys into Elasticsearch") {
      val bulkResponse = BulkResponse(1, errors = false, List(
        BulkResponseItems(Some(BulkResponseItem(1, "key1", "someStorage", "_doc", 1, forcedRefresh = false, found = true,created = true, "someResult", 200, None, None)),
          None, None, None),
        BulkResponseItems(Some(BulkResponseItem(1, "key2", "someStorage", "_doc", 2, forcedRefresh = false, found = true,created = true, "someResult", 200, None, None)),
          None, None, None),
        BulkResponseItems(Some(BulkResponseItem(1, "key3", "someStorage", "_doc", 3, forcedRefresh = false, found = true,created = true, "someResult", 200, None, None)),
          None, None, None)))
      getBulkIndexRequest(fakeClient).returning(getRequestSuccessFuture(bulkResponse))

      elasticsearchKvProcessor.set("someStorage", Map(
        "key1" -> "value1",
        "key2" -> "value2",
        "key3" -> "value3"
      ))
        .map { iterable => assert(iterable.forall(v => v._2)) }
    }
    it("set(kvs) should fail if execute method fails") {
      getBulkIndexRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.set("someStorage", Map(
          "key1" -> "value1",
          "key2" -> "value2",
          "key3" -> "value3"
        ))
      }
    }
    it("set(kvs) future should fail if request fails") {
      getBulkIndexRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.set("someStorage", Map(
          "key1" -> "value1",
          "key2" -> "value2",
          "key3" -> "value3"
        ))
      }
    }
    //set(storage, kvs) test end

    //delete(storage, key) test start
    it("should delete existing value by key from Elasticsearch") {
      val deleteResponse = DeleteResponse(Shards(1, 0, 1),"someStorage", "_doc","someKey", 1, "someResult")
      getDeleteRequest(fakeClient).returning(getRequestSuccessFuture(deleteResponse))

      elasticsearchKvProcessor.delete("someStorage", "someKey").map { value => assert(value) }
    }
    it("delete(key) should fail if execute method fails") {
      getDeleteRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.delete("someStorage", "someKey")
      }
    }
    it("delete(key) future should fail if request fails") {
      getDeleteRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.delete("someStorage", "someKey")
      }
    }
    //delete(storage, key) test end

    //delete(storage, keys) test start
    it("should delete multiple values by keys into Elasticsearch") {
      val bulkResponse = BulkResponse(1, errors = false, List(
        BulkResponseItems(None, Some(BulkResponseItem(1, "key1", "someStorage", "_doc", 1, forcedRefresh = false, found = true, created = false, "someResult", 200, None, None)),
          None, None),
        BulkResponseItems(None, Some(BulkResponseItem(1, "key2", "someStorage", "_doc", 2, forcedRefresh = false, found = true, created = false, "someResult", 200, None, None)),
          None, None),
        BulkResponseItems(None, Some(BulkResponseItem(1, "key3", "someStorage", "_doc", 3, forcedRefresh = false, found = true, created = false, "someResult", 200, None, None)),
          None, None)))
      getBulkDeleteRequest(fakeClient).returning(getRequestSuccessFuture(bulkResponse))

      elasticsearchKvProcessor.delete("someStorage", Set("key1", "key2", "key3"))
        .map { iterable => assert(iterable.forall(v => v._2)) }
    }
    it("delete(keys) should fail if execute method fails") {
      getBulkDeleteRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.delete("someStorage", Set("key1", "key2", "key3"))
      }
    }
    it("delete(keys) future should fail if request fails") {
      getBulkDeleteRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.delete("someStorage", Set("key1", "key2", "key3"))
      }
    }
    //delete(storage, keys) test end

    //list(storage) test start
    it("should list keys and values existing in Elasticsearch") {
      val searchResponse = SearchResponse(1, isTimedOut = false, isTerminatedEarly = false, Map.empty, Shards(1, 0, 1), None, Map.empty, SearchHits(3, 1, Array(
        SearchHit("key1", "someStorage", "_doc", 1, 1, None, None, None, None, None, None, Map("value" -> "value1"), Map("value" -> "value1"), Map.empty, Map.empty),
        SearchHit("key2", "someStorage", "_doc", 1, 1, None, None, None, None, None, None, Map("value" -> "value2"), Map("value" -> "value2"), Map.empty, Map.empty),
        SearchHit("key3", "someStorage", "_doc", 1, 1, None, None, None, None, None, None, Map("value" -> "value3"), Map("value" -> "value3"), Map.empty, Map.empty)
      )))
      getSearchRequest(fakeClient).returning(getRequestSuccessFuture(searchResponse))
      elasticsearchKvProcessor.list("someStorage")
        .map { values => assert(values == Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3")) }
    }
    it("list should fail if execute method fails") {
      getSearchRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.list("someStorage")
      }
    }
    it("list future should fail if request fails") {
      getSearchRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.list("someStorage")
      }
    }
    //list(storage) test end

    //clear(storage) test start
    it("should clear targeted storage in Elasticsearch") {
      val deleteByQueryResponse = DeleteByQueryResponse(1, timedOut = false, 3, 3, 1, 0, 0, 0, 3, 0)
      getDeleteByQueryRequest(fakeClient).returning(getRequestSuccessFuture(deleteByQueryResponse))
      elasticsearchKvProcessor.clear("someStorage")
        .map { value => assert(value) }
    }
    it("clear should fail if execute method fails") {
      getDeleteByQueryRequest(fakeClient).throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.clear("someStorage")
      }
    }
    it("clear future should fail if request fails") {
      getDeleteByQueryRequest(fakeClient).returning(getRequestFailureFuture)
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.clear("someStorage")
      }
    }
    //clear(storage) test end

  }
}
