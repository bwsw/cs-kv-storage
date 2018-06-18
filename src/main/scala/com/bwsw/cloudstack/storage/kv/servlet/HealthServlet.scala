package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.util.HealthChecker
import org.scalatra._

import scala.concurrent.{ExecutionContext, Future}

class HealthServlet(system: ActorSystem, checker: HealthChecker)
  extends ScalatraServlet
    with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/") {
    new AsyncResult() {
      val is: Future[_] = checker.check
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
