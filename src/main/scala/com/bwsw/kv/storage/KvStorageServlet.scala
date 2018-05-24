package com.bwsw.kv.storage

import org.scalatra._
import akka.actor.ActorSystem
import com.bwsw.kv.storage.error._
import com.bwsw.kv.storage.processor.KvProcessor
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future}

case class SimpleValue(value: String)
case class MapValue(map: Map[String, String])
case class Keys(keys: List[String])

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
      override val is: Future[_] = processor.get(params("storage_uuid").toString, params("key").toString)
        .map {
          case Left(error: NotFoundError) => NotFound()
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => value
        }
    }
  }

  post("/:storage_uuid/") {
    new AsyncResult() {
      override val is: Future[_] = processor.get(params("storage_uuid").toString, parsedBody.extract[Keys].keys)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => value
        }
    }
  }

  put("/:storage_uuid/:key") {
    new AsyncResult() {
      override val is: Future[_] = processor.set(params("storage_uuid").toString, params("key").toString, parsedBody.extract[SimpleValue].value)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => Ok()
          case _ => BadRequest
        }
    }
  }

  put("/:storage_uuid/") {
    new AsyncResult() {
      override val is: Future[_] = processor.set(params("storage_uuid").toString, parsedBody.extract[Map[String, String]])
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => value
        }
    }
  }

  delete("/:storage_uuid/:key") {
    new AsyncResult() {
      override val is: Future[_] = processor.delete(params("storage_uuid").toString, params("key").toString)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => Ok()
        }
    }
  }

  delete("/:storage_uuid/") {
    new AsyncResult() {
      override val is: Future[_] = processor.get(params("storage_uuid").toString, parsedBody.extract[Keys].keys)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => value
        }
    }
  }

  get("/:storage_uuid/list") {
    new AsyncResult() {
      override val is: Future[_] = processor.list(params("storage_uuid").toString)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Right(value) => value
        }
    }

  }
  post("/:storage_uuid/clear") {
    new AsyncResult() {
      override val is: Future[_] = processor.clear(params("storage_uuid").toString)
        .map {
          case Left(error: InternalError) => InternalServerError()
          case Left(error: ConflictError) => Conflict()
          case Right(value) => Ok()
        }
    }
  }

}
