package com.bwsw.kv.storage.kvprocessor

import com.bwsw.kv.storage.models.kvprocessor.ElasticsearchKvProcessor
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http._

import scala.util.{Failure, Success}
import org.scalamock.matchers.MockParameter
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpec

import scala.concurrent.Future

class ElasticsearchKvProcessorUnitTest extends FunSpec with MockFactory {
//  describe("A ElasticsearchKvProcessor") {
//    val fakeClient = stub[HttpClient]
//    val elasticsearchKvProcessor = new ElasticsearchKvProcessor(fakeClient)
//    val getRequest = (fakeClient.execute _).expects(new MockParameter(ElasticDsl.get("someKey").from("someStorage" / "_doc")))
//    it("should get existing value by key from Elasticsearch"){
////      val requestFailure: Either[RequestFailure, RequestSuccess[Nothing]] = Left(stub[Nothing])
//      val requestSuccess = RequestSuccess(getResponse)
//      val getResponse = stub[GetResponse]
//      (requestSuccess.result _).expects().returning(getResponse)
//      getRequest.returning(Future(Right(requestSuccess)))
//      elasticsearchKvProcessor.get("someStorage", "someKey")
//
//    }
//    it("should fail if execute method fails"){
//      //      val requestFailure: Either[RequestFailure, RequestSuccess[Nothing]] = Left(stub[Nothing])
//      getRequest.throwing(new Exception("something went wrong"))
//      assertThrows[Exception]{
//        elasticsearchKvProcessor.get("someStorage", "someKey")
//      }
//    }
//    it("should fail if request fails"){
//      RequestFailure(404, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))
//    }
//  }
}
