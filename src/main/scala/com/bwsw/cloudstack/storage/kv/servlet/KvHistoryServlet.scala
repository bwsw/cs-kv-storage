package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.error.NotFoundError
import com.bwsw.cloudstack.storage.kv.processor.HistoryProcessor
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future}

class KvHistoryServlet(system: ActorSystem, processor: HistoryProcessor)
  extends ScalatraServlet
    with FutureSupport
    with JacksonJsonSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues

  //  get("/:storage_uuid") {
  //    new AsyncResult() {
  //      val is: Future[_] = {
  //        processor.get(params).map {
  //          case Right(value) =>
  //            contentType = formats("json")
  //            value
  //          case Left(_: NotFoundError) => NotFound("")
  //          case _ => InternalServerError()
  //        }
  //      }
  //    }
  //  }
}
