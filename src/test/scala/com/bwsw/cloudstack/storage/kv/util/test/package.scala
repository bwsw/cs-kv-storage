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

package com.bwsw.cloudstack.storage.kv.util

import com.sksamuel.elastic4s.http.{ElasticError, RequestFailure, RequestSuccess}

import scala.concurrent.{ExecutionContext, Future}

package object test {
  def getRequestSuccessFuture[T](response: T)
    (implicit executionContext: ExecutionContext): Future[Right[RequestFailure, RequestSuccess[T]]] = Future(Right(
    RequestSuccess(200, Option.empty, Map.empty, response)))

  def getRequestFailureFuture[T](statusCode: Int = 500)
    (implicit executionContext: ExecutionContext): Future[Left[RequestFailure, RequestSuccess[T]]] = Future(Left(
    RequestFailure(statusCode, Option.empty, Map.empty, ElasticError.fromThrowable(new RuntimeException()))))
}
