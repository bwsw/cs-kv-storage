package com.bwsw.kv.storage

import org.scalatra._
import akka.actor.ActorSystem
import com.bwsw.kv.storage.error._
import com.bwsw.kv.storage.processor.KvProcessor
import javax.swing.JList
import org.json4s.JsonAST.{JArray, JNothing}
import org.json4s.{DefaultFormats, Formats, JObject, JSet}
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future}

class KvStorageServlet(system: ActorSystem, processor: KvProcessor)
  extends ScalatraServlet
  with FutureSupport
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == "text/plain")
          processor.get(params("storage_uuid"), params("key"))
          .map {
            case Left(_: NotFoundError) => NotFound()
            case Right(value) => value
            case _ => InternalServerError()
          }
        else
          Future(BadRequest())
    }
  }

  post("/:storage_uuid/") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              processor.get(params("storage_uuid"), json.extract[List[String]])
                .map {
                  case Right(value) => value
                  case _ => InternalServerError()
                }
            case JNothing => Future(BadRequest())
            case _ => Future(InternalServerError()) }
        else
          Future(BadRequest())
    }
  }

  put("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == "text/plain" && request.body.nonEmpty)
          processor.set(params("storage_uuid"), params("key"), request.body)
          .map {
            case Right(_) => Ok()
            case Left(_: BadRequestError) => BadRequest()
            case _ => InternalServerError()
          }
        else
          Future(BadRequest())
    }
  }

  put("/:storage_uuid/set") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JObject =>
              processor.set(params("storage_uuid"), json.extract[Map[String, String]])
                .map {
                  case Right(value) => value
                  case Left(_: BadRequestError) => BadRequest()
                  case _ => InternalServerError()
                }
            case JNothing => Future(BadRequest())
            case _ => Future(InternalServerError()) }
        else
          Future(BadRequest())
    }
  }

  delete("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == "text/plain")
          processor.delete(params("storage_uuid"), params("key"))
          .map {
            case Right(_) => Ok()
            case _ => InternalServerError()
          }
        else
          Future(BadRequest())
    }
  }

  put("/:storage_uuid/delete") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              processor.delete(params("storage_uuid"), json.extract[List[String]])
                .map {
                  case Right(value) => value
                  case _ => InternalServerError()
                }
            case JNothing => Future(BadRequest())
            case _ => Future(InternalServerError()) }
        else
          Future(BadRequest())
    }
  }

  get("/:storage_uuid/list") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == "text/plain")
          processor.list(params("storage_uuid"))
          .map {
            case Right(value) =>
              contentType = formats("json")
              value
            case _ => InternalServerError()
          }
        else
          Future(BadRequest())
    }

  }

  put("/:storage_uuid/clear") {
    new AsyncResult() {
      val is: Future[_] =
        if(request.getHeader("Content-Type") == "text/plain")
          processor.clear(params("storage_uuid"))
          .map {
            case Left(_: ConflictError) => Conflict()
            case Right(_) => Ok()
            case _ => InternalServerError()
          }
        else
          Future(BadRequest())
    }
  }

}
