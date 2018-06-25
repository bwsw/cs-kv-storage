package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.processor.HealthProcessor
import org.scalatra._

import scala.concurrent.{ExecutionContext, Future}

class HealthServlet(system: ActorSystem, healthProcessor: HealthProcessor)
  extends ScalatraServlet
    with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/") {
    new AsyncResult() {
      val is: Future[_] = healthProcessor.check
        .map {
          case Right(true) =>
            Ok("")
          case Right(false) =>
            InternalServerError("")
          case _ => InternalServerError()
        }
    }
  }
}
