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

import com.bwsw.cloudstack.storage.kv.entity._
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

  /** Searches for operation history of the storage by specified criteria.
    *
    * @param storageUuid UUID of the storage
    * @param keys        keys
    * @param operations  operations
    * @param start       the start timestamp of the period
    * @param end         the end timestamp of the period
    * @param sort        fields to sort results
    * @param page        the page of results
    * @param size        amount of results for one page/batch
    * @param scroll      time in ms to keep the search context open for subsequent scroll requests
    * @return a [[Future]] with results or StorageError if some error occurs
    */
  def get(
      storageUuid: String,
      keys: Set[String],
      operations: Set[Operation],
      start: Option[Long],
      end: Option[Long],
      sort: Set[SortField],
      page: Option[Int],
      size: Option[Int],
      scroll: Option[Long]): Future[Either[StorageError, SearchResult[History]]]

  /** Retrieves the next batch of results.
    *
    * @param scrollId scroll id
    * @param timeout  time in ms to keep the search context open for subsequent scroll requests
    * @return a [[Future]] with results or StorageError if some error occurs
    */
  def scroll(scrollId: String, timeout: Long): Future[Either[StorageError, ScrollSearchResult[History]]]
}
