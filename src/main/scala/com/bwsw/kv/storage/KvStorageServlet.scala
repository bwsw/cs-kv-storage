package com.bwsw.kv.storage

import akka.actor.ActorSystem
import com.bwsw.kv.storage.error._
import com.bwsw.kv.storage.processor.KvProcessor
import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, Formats, JObject, MappingException}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.{ExecutionContext, Future}

class KvStorageServlet(system: ActorSystem, processor: KvProcessor)
  extends ScalatraServlet
    with FutureSupport
    with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = processor.get(params("storage_uuid"), params("key"))
        .map {
          case Left(_: NotFoundError) => NotFound()
          case Right(value) =>
            contentType = formats("txt")
            value
          case _ => InternalServerError()
        }
    }
  }

  post("/:storage_uuid/") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              try {
                processor.get(params("storage_uuid"), json.extract[List[String]])
                  .map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case _ => InternalServerError()
                  }
              } catch {
                case ma: MappingException => Future(BadRequest())
              }
            case _ => Future(BadRequest())
          }
        else
          Future(BadRequest())
    }
  }

  put("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("txt"))
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
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JObject =>
              try {
                processor.set(params("storage_uuid"), json.extract[Map[String, String]])
                  .map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case _ => InternalServerError()
                  }
              } catch {
                case e: MappingException => Future(BadRequest())
              }
            case _ => Future(BadRequest())
          }
        else
          Future(BadRequest())
    }
  }

  delete("/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        processor.delete(params("storage_uuid"), params("key"))
          .map {
            case Right(_) => Ok()
            case _ => InternalServerError()
          }
    }
  }

  put("/:storage_uuid/delete") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              try {
                processor.delete(params("storage_uuid"), json.extract[List[String]])
                  .map {
                    case Right(value) =>
                      contentType = formats("json")
                      value
                    case _ => InternalServerError()
                  }
              } catch {
                case e: MappingException => Future(BadRequest)
              }
            case _ => Future(BadRequest())
          }
        else
          Future(BadRequest())
    }
  }

  get("/:storage_uuid/list") {
    new AsyncResult() {
      val is: Future[_] =
        processor.list(params("storage_uuid"))
          .map {
            case Right(value) =>
              contentType = formats("json")
              value
            case _ => InternalServerError()
          }
    }
  }

  put("/:storage_uuid/clear") {
    new AsyncResult() {
      val is: Future[_] =
        processor.clear(params("storage_uuid"))
          .map {
            case Left(_: ConflictError) => Conflict()
            case Right(_) => Ok()
            case _ => InternalServerError()
          }
    }
  }
}
