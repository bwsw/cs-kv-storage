package com.bwsw.cloudstack.storage.kv.app

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.manager.ElasticsearchKvStorageManager
import com.bwsw.cloudstack.storage.kv.processor.ElasticsearchKvProcessor
import com.bwsw.cloudstack.storage.kv.servlet.{KvStorageManagerServlet, KvStorageServlet}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import javax.servlet.ServletContext
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf = new Configuration
  val client = HttpClient(ElasticsearchClientUri(conf.getElasticsearchUri))
  val processor = new ElasticsearchKvProcessor(client, conf)
  val manager = new ElasticsearchKvStorageManager(client)
  val system = ActorSystem()

  override def init(context: ServletContext) {
    context.mount(new KvStorageManagerServlet(system, manager), "/storage/*")
    context.mount(new KvStorageServlet(system, processor), "/*")
  }

  override def destroy(context: ServletContext) {
    system.terminate
  }
}
