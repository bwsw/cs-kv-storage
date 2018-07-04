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

package com.bwsw.cloudstack.storage.kv.processor

import com.bwsw.cloudstack.storage.kv.entity.{History, Operation, SearchResponseBody, SearchScrolledBody}
import com.bwsw.cloudstack.storage.kv.error.StorageError
import com.bwsw.cloudstack.storage.kv.message._

import scala.concurrent.Future

/** A processor of storage histories. **/
trait HistoryProcessor {
  /** Saves collection a historical records into dedicated storage
    *
    * @param histories a Collection of histories
    * @return a [[Future]] with a boolean operation status for each key or error
    */
  def save(histories: List[KvHistory]): Future[Option[List[KvHistory]]]

  /** Searches for history records suitable to parameters gotten
    *
    * @param storageUuid UUID of storage to search from
    * @return a [[Future]] of response body or StorageError if any error occurred
    */
  def get(
      storageUuid: String,
      keys: Iterable[String],
      operations: Iterable[Operation],
      start: Option[Long],
      end: Option[Long],
      sort: Iterable[String],
      page: Option[Int],
      size: Option[Int],
      scroll: Option[Long]): Future[Either[StorageError, SearchResponseBody[History]]]

  /** Retrieves a single page of the request
    *
    * @param scrollId Id of the scroll
    * @param timeout  time to keep the search context open for in milliseconds
    * @return a [[Future]] of response body or StorageError if any error occurred
    */
  def scroll(scrollId: String, timeout: Long): Future[Either[StorageError, SearchScrolledBody[History]]]
}
