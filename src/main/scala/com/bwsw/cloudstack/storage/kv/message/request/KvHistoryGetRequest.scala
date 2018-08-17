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

package com.bwsw.cloudstack.storage.kv.message.request

import com.bwsw.cloudstack.storage.kv.entity.{Operation, SortField}

case class KvHistoryGetRequest(
    storageUuid: String,
    secretKey: Array[Char],
    keys: Set[String],
    operations: Set[Operation],
    start: Option[Long],
    end: Option[Long],
    sort: Set[SortField],
    page: Option[Int],
    size: Option[Int],
    scroll: Option[Long]) {

  /** @inheritdoc */
  override def equals(o: scala.Any): Boolean = o match {
    case request: KvHistoryGetRequest =>
      request.storageUuid == this.storageUuid &&
        (request.secretKey == null && this.secretKey == null || request.secretKey != null && this
          .secretKey != null && request.secretKey.sameElements(this.secretKey)) &&
        request.keys == this.keys &&
        request.operations == this.operations &&
        request.start == this.start &&
        request.end == this.end &&
        request.sort == this.sort &&
        request.page == this.page &&
        request.size == this.size &&
        request.scroll == this.scroll
    case _ => false
  }
}
