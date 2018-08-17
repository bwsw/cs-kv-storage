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

case class KvSetRequest(storage: String, secretKey: Array[Char], key: String, value: String) extends KvRequest {
  /** @inheritdoc */
  override def equals(o: scala.Any): Boolean = o match {
    case request: KvSetRequest =>
      request.storage == this.storage &&
        request.key == this.key &&
        request.value == this.value &&
        (request.secretKey == null && this.secretKey == null || request.secretKey != null && this
          .secretKey != null && request.secretKey.sameElements(this.secretKey))
    case _ => false
  }
}
