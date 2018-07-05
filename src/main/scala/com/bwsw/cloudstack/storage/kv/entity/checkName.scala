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

package com.bwsw.cloudstack.storage.kv.entity

sealed trait CheckName

object StorageRegistry extends CheckName {
  override def toString: String = "STORAGE_REGISTRY"
}

object StorageTemplate extends CheckName {
  override def toString: String = "STORAGE_TEMPLATE"
}

object HistoryStorageTemplate extends CheckName {
  override def toString: String = "HISTORY_STORAGE_TEMPLATE"
}

object Unspecified extends CheckName {
  override def toString: String = "UNSPECIFIED"
}

object CheckName {
  def parse(string: String): CheckName = string match {
    case "STORAGE_REGISTRY" => StorageRegistry
    case "STORAGE_TEMPLATE" => StorageTemplate
    case "HISTORY_STORAGE_TEMPLATE" => HistoryStorageTemplate
    case _ => throw new IllegalArgumentException
  }
}
