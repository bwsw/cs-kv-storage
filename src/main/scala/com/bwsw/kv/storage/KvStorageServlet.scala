package com.bwsw.kv.storage

import org.scalatra._
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout


import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KvStorageServlet extends ScalatraServlet { //with FutureSupport{
//  implicit val timeout = new Timeout(2 seconds)
//  protected implicit def executor: ExecutionContext = system.dispatcher


//  get("/list/:storage_uuid"){
//    new AsyncResult { val is =
//      gateway ? "list"
//    }
//  }
}