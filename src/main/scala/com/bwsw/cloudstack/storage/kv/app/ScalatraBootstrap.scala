package com.bwsw.cloudstack.storage.kv.app

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.actor.KvActor
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
  private val configuration = inject[Configuration]

  override def init(context: ServletContext) {
    context.mount(new KvStorageManagerServlet(system, kvManager), "/storage/*")
    context.mount(new KvStorageServlet(system, configuration.getRequestTimeout, kvProcessor, kvActor), "/*")
  }

  override def destroy(context: ServletContext) {
    system.terminate
  }
}
