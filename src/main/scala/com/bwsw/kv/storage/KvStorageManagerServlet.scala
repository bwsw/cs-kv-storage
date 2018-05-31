package com.bwsw.kv.storage

import akka.actor.ActorSystem
import com.bwsw.kv.storage.error.{BadRequestError, NotFoundError}
import com.bwsw.kv.storage.manager.KvStorageManager
import org.scalatra._

import scala.concurrent.{ExecutionContext, Future}

class KvStorageManagerServlet(system: ActorSystem, manager: KvStorageManager) extends ScalatraServlet
  with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  put("/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] = {
        val ttl = params.get("ttl")
        if (ttl.nonEmpty)
          try {
            manager.updateTempStorageTtl(params("storage_uuid"), ttl.get.toLong)
              .map {
                case Right(_) => Ok()
                case Left(_: BadRequestError) => BadRequest()
                case Left(_: NotFoundError) => NotFound()
                case _ => InternalServerError()
              }
          } catch {
            case e: NumberFormatException => Future(BadRequest())
          }
        else
          Future(BadRequest())
      }
    }
  }

  delete("/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        manager.deleteTempStorage(params("storage_uuid"))
          .map {
            case Right(_) => Ok()
            case Left(_: BadRequestError) => BadRequest()
            case Left(_: NotFoundError) => NotFound()
            case _ => InternalServerError()
          }
    }
  }
}
