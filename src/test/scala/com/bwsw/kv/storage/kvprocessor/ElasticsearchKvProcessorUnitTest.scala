package com.bwsw.kv.storage.kvprocessor

import com.bwsw.kv.storage.models.kvprocessor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.get.{GetDefinition, MultiGetDefinition}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.{GetResponse, MultiGetResponse}
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.indexes.IndexDefinition
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpec
import org.scalatest.RecoverMethods._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class ElasticsearchKvProcessorUnitTest extends FunSpec with MockFactory {
  describe("A ElasticsearchKvProcessor") {
    val fakeClient = mock[HttpClient]
    val elasticsearchKvProcessor = new ElasticsearchKvProcessor(fakeClient)

    //get(storage, key) test start
    it("should get existing value by key from Elasticsearch"){
      val getRequest = (fakeClient.execute[GetDefinition, GetResponse] (_:GetDefinition)(_:HttpExecutable[GetDefinition, GetResponse], _ : ExecutionContext))
        .expects(ElasticDsl.get("someKey").from("someStorage" / "_doc"), GetHttpExecutable, *)
      val getResponse = GetResponse("someKey", "someStorage", "_doc", 1, true, Map("value" -> "someValue"), Map("value" -> "someValue"))
      val requestSuccess = RequestSuccess(200, Option.empty, Map.empty, getResponse)
      getRequest.returning(Future(Right(requestSuccess)))

      elasticsearchKvProcessor.get("someStorage", "someKey").map{ value => assert(value == "someValue")}
    }
    it("get should fail if execute method fails"){
      val getRequest = (fakeClient.execute[GetDefinition, GetResponse] (_:GetDefinition)(_:HttpExecutable[GetDefinition, GetResponse], _ : ExecutionContext))
        .expects(ElasticDsl.get("someKey").from("someStorage" / "_doc"), GetHttpExecutable, *)
      getRequest.throwing(new Exception())
      assertThrows[Exception] {
        elasticsearchKvProcessor.get("someStorage", "someKey")
      }
    }
    it("future obtained by get should fail if request fails"){
      val getRequest = (fakeClient.execute[GetDefinition, GetResponse] (_:GetDefinition)(_:HttpExecutable[GetDefinition, GetResponse], _ : ExecutionContext))
        .expects(ElasticDsl.get("someKey").from("someStorage" / "_doc"), GetHttpExecutable, *)
      getRequest.returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.get("someStorage", "someKey")
      }
    }
    //get(storage, key) test end

    //get(storage, keys) test start
    val gets = Set(
      ElasticDsl.get("key1").from("someStorage" / "_doc"),
      ElasticDsl.get("key2").from("someStorage" / "_doc"),
      ElasticDsl.get("key3").from("someStorage" / "_doc"))
    it("should get multiple existing values by keys from Elasticsearch"){
      val multiGetRequest = (fakeClient.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
        .expects(multiget(gets), MultiGetHttpExecutable, *)
      val multiGetResponse = MultiGetResponse(Seq(
        GetResponse("key1", "someStorage", "_doc", 1, true, Map("value" -> "value1"), Map("value" -> "value1")),
        GetResponse("key2", "someStorage", "_doc", 1, true, Map("value" -> "value2"), Map("value" -> "value2")),
        GetResponse("key3", "someStorage", "_doc", 1, true, Map("value" -> "value3"), Map("value" -> "value3"))))
      val requestSuccess = RequestSuccess(200, Option.empty, Map.empty, multiGetResponse)
      multiGetRequest.returning(Future(Right(requestSuccess)))

      elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
        .map{ values => assert(values == Map("key1" -> "value1", "key2" -> "value2", "key3" -> "value3"))}
    }
    it("multiGet should fail if execute method fails"){
      val multiGetRequest = (fakeClient.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
        .expects(multiget(gets), MultiGetHttpExecutable, *)
      multiGetRequest.throwing(new Exception())
      assertThrows[Exception]{
        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
      }
    }
    it("multiGet should fail if request fails"){
      val multiGetRequest = (fakeClient.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
        .expects(multiget(gets), MultiGetHttpExecutable, *)
      multiGetRequest.returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
      recoverToSucceededIf[RuntimeException] {
        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
      }
    }
    //get(storage, keys) test end

    //set(storage, key) test start
    it("should set value by key into Elasticsearch"){
      val indexRequest = (fakeClient.execute[IndexDefinition, IndexResponse] (_:IndexDefinition)(_:HttpExecutable[IndexDefinition, IndexResponse], _ : ExecutionContext))
        .expects(indexInto("someStorage" / "_doc") id "someKey" fields ("value" -> "someValue" ), IndexHttpExecutable, *)
      val indexResponse = IndexResponse("someKey", "someStorage", "_doc", 1, "someValue", true, Shards(1, 1, 1))
      val requestSuccess = RequestSuccess(200, Option.empty, Map.empty, indexResponse)
      indexRequest.returning(Future(Right(requestSuccess)))

      elasticsearchKvProcessor.set("someStorage", "someKey", "someValue")
        .map{ value => assert(value)}
    }
//    it("multiGet should fail if execute method fails"){
//      val multiGetRequest = (fakeClient.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
//        .expects(multiget(gets), MultiGetHttpExecutable, *)
//      multiGetRequest.throwing(new Exception())
//      assertThrows[Exception]{
//        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
//      }
//    }
//    it("multiGet should fail if request fails"){
//      val multiGetRequest = (fakeClient.execute[MultiGetDefinition, MultiGetResponse] (_:MultiGetDefinition)(_:HttpExecutable[MultiGetDefinition, MultiGetResponse], _ : ExecutionContext))
//        .expects(multiget(gets), MultiGetHttpExecutable, *)
//      multiGetRequest.returning(Future(Left(RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException())))))
//      recoverToSucceededIf[RuntimeException] {
//        elasticsearchKvProcessor.get("someStorage", Set("key1", "key2", "key3"))
//      }
//    }
    //set(storage, key) test end
  }
}
