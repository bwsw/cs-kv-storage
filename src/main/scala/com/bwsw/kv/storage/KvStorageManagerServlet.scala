package com.bwsw.kv.storage

import akka.actor.ActorSystem
import com.bwsw.kv.storage.error.{BadRequestError, NotFoundError}
import com.bwsw.kv.storage.manager.KvStorageManager
import org.scalatra._

import scala.concurrent.{ExecutionContext, Future}

class KvStorageManagerServlet(system: ActorSystem, manager: KvStorageManager) extends ScalatraServlet with FutureSupport {

  protected implicit def executor: ExecutionContext = system.dispatcher

  put("/storage/:storage_uuid") {
    new AsyncResult() {
      val is: Future[_] =
        if(params("ttl").nonEmpty)
          manager.updateTempStorageTtl(params("storage_uuid"), params("ttl").toLong)
            .map {
              case Right(_) => Ok()
              case Left(_: BadRequestError) => BadRequest()
              case Left(_: NotFoundError) => NotFound()
              case _ => InternalServerError()
            }
        else
          Future(BadRequest())
    }
  }
  delete("/storage/:storage_uuid") {
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
