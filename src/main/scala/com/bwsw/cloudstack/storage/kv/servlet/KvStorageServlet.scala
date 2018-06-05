package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, ConflictError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.message.request.KvMultiGetRequest
import com.bwsw.cloudstack.storage.kv.processor.KvProcessor
import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, Formats, JObject, MappingException}
import org.scalatra._
import org.scalatra.json.JacksonJsonSupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import com.bwsw.cloudstack.storage.kv.message.request._

class KvStorageServlet(system: ActorSystem, requestTimeout: FiniteDuration, kvProcessor: KvProcessor, kvActor: ActorRef)
  extends ScalatraServlet
    with FutureSupport
    with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats.preservingEmptyValues
  protected implicit val akkaTimeout: Timeout = requestTimeout

  protected implicit def executor: ExecutionContext = system.dispatcher

  get("/get/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = (kvActor ? KvGetRequest(params("storage_uuid"), params("key")))
        .map {
          case Left(_: NotFoundError) => NotFound("")
          case Right(value) =>
            contentType = formats("txt")
            value
          case _ => InternalServerError()
        }
    }
  }

  post("/get/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              try {
                val result = kvActor ? KvMultiGetRequest(params("storage_uuid"), json.extract[List[String]])
                result.map {
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

  put("/set/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("txt")) {
          val result = kvActor ? KvSetRequest(params("storage_uuid"), params("key"), request.body)
          result.map {
            case Right(_) => Ok()
            case Left(_: BadRequestError) => BadRequest()
            case _ => InternalServerError()
          }
        }
        else
          Future(BadRequest())
    }
  }

  put("/set/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JObject =>
              try {
                val result = kvActor ? KvMultiSetRequest(params("storage_uuid"), json.extract[Map[String, String]])
                result.map {
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

  delete("/delete/:storage_uuid/:key") {
    new AsyncResult() {
      val is: Future[_] = {
        val result = kvActor ? KvDeleteRequest(params("storage_uuid"), params("key"))
        result.map {
          case Right(_) => Ok()
          case _ => InternalServerError()
        }
      }
    }
  }

  post("/delete/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if (request.getHeader("Content-Type") == formats("json"))
          parsedBody match {
            case json: JArray =>
              try {
                val result = kvActor ? KvMultiDeleteRequest(params("storage_uuid"), json.extract[List[String]])
                result.map {
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

  get("/list/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = {
        val result = kvActor ? KvListRequest(params("storage_uuid"))
        result.map {
          case Right(value) =>
            contentType = formats("json")
            value
          case _ => InternalServerError()
        }
      }
    }
  }

  post("/clear/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = {
        val result = kvActor ? KvClearRequest(params("storage_uuid"))
        result.map {
          case Left(_: ConflictError) => Conflict()
          case Right(_) => Ok()
          case _ => InternalServerError()
        }
      }
    }
  }

}
