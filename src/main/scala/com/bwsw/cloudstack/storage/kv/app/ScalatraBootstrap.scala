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
import com.bwsw.cloudstack.storage.kv.actor.KvActor
import com.bwsw.cloudstack.storage.kv.configuration.AppConfig
import com.bwsw.cloudstack.storage.kv.manager.KvStorageManager
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import com.bwsw.cloudstack.storage.kv.servlet.{KvStorageManagerServlet, KvStorageServlet}
import javax.servlet.ServletContext
import org.scalatra._
import scaldi.akka.AkkaInjectable._


class ScalatraBootstrap extends LifeCycle {

  implicit val system: ActorSystem = ActorSystem("cs-kv-storage")
  implicit val module: KvStorageModule = new KvStorageModule

  private val kvManager = inject[KvStorageManager]
  private val kvProcessor = inject[KvProcessor]
  private val kvActor = injectActorRef[KvActor]
  private val appConfig = inject[AppConfig]

  override def init(context: ServletContext) {
    context.mount(new KvStorageManagerServlet(system, kvManager), "/storage/*")
    context.mount(new KvStorageServlet(system, appConfig.getRequestTimeout, kvProcessor, kvActor), "/*")
  }

  override def destroy(context: ServletContext) {
    system.terminate
  }
}
