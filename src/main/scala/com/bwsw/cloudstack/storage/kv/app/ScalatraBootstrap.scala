// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
