package com.bwsw.kv.storage

import org.scalatra._
import akka.actor.ActorSystem
import com.bwsw.kv.storage.error._
import com.bwsw.kv.storage.processor.KvProcessor
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future}

class KvStorageServlet(system: ActorSystem, processor: KvProcessor)
  extends ScalatraServlet
  with FutureSupport
  with JacksonJsonSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

  get("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = processor.get(params("storage_uuid"), params("key"))
        .map {
          case Left(_: NotFoundError) => NotFound()
          case Right(value) => value
          case _ => InternalServerError()
        }
    }
  }

  post("/:storage_uuid/") {
    new AsyncResult() {
      val is: Future[_] = processor.get(params("storage_uuid"), parsedBody.extract[List[String]])
        .map {
          case Right(value) => value
          case _ => InternalServerError()
        }
    }
  }

  put("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = processor.set(params("storage_uuid"), params("key"), parsedBody.extract[String])
        .map {
          case Right(_) => Ok()
          case _ => InternalServerError()
        }
    }
  }

  put("/:storage_uuid/set") {
    new AsyncResult() {
      val is: Future[_] = processor.set(params("storage_uuid"), parsedBody.extract[Map[String, String]])
        .map {
          case Right(value) => value
          case _ => InternalServerError()
        }
    }
  }

  delete("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = processor.delete(params("storage_uuid"), params("key"))
        .map {
          case Right(_) => Ok()
          case _ => InternalServerError()
        }
    }
  }

  put("/:storage_uuid/delete") {
    new AsyncResult() {
      val is: Future[_] = processor.delete(params("storage_uuid"), parsedBody.extract[List[String]])
        .map {
          case Right(value) => value
          case _ => InternalServerError()
        }
    }
  }

  get("/:storage_uuid/list") {
    new AsyncResult() {
      val is: Future[_] = processor.list(params("storage_uuid"))
        .map {
          case Right(value) => value
          case _ => InternalServerError()
        }
    }

  }

  put("/:storage_uuid/clear") {
    new AsyncResult() {
      val is: Future[_] = processor.clear(params("storage_uuid"))
        .map {
          case Left(_: ConflictError) => Conflict()
          case Right(_) => Ok()
          case _ => InternalServerError()
        }
    }
  }

}
