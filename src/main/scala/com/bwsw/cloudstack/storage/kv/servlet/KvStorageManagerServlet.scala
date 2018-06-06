// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.servlet

import akka.actor.ActorSystem
import com.bwsw.cloudstack.storage.kv.error.{BadRequestError, NotFoundError}
import com.bwsw.cloudstack.storage.kv.manager.KvStorageManager
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
