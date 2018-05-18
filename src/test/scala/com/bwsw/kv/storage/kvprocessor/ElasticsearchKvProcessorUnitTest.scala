package com.bwsw.kv.storage.kvprocessor

import com.bwsw.kv.storage.models.kvprocessor.ElasticsearchKvProcessor
import org.apache.http.Header
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.client.RestHighLevelClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSpec

class ElasticsearchKvProcessorUnitTest extends FunSpec with MockFactory {
  describe("A ElasticsearchKvProcessor") {
    val fakeClient = stub[RestHighLevelClient]
//    fakeClient.getAsync _ expects (one: GetRequest, two: ActionListener[GetResponse], three: Seq[Header]) onCall { one, two, three => throw new RuntimeException(arg) }
    val elasticsearchKvProcessor = new ElasticsearchKvProcessor(fakeClient)
    it("should get existing value by key from Elasticsearch"){
//      elasticsearchKvProcessor.get("1", "somekey")

    }
  }
}
