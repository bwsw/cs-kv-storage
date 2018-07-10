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

object CheckName {

  private val StorageRegistryValue = "STORAGE_REGISTRY"
  private val StorageTemplateValue = "STORAGE_DATA_TEMPLATE"
  private val HistoryStorageTemplateValue = "STORAGE_HISTORY_TEMPLATE"

  object StorageRegistry extends CheckName {
    override def toString: String = StorageRegistryValue
  }

  object StorageTemplate extends CheckName {
    override def toString: String = StorageTemplateValue
  }

  object HistoryStorageTemplate extends CheckName {
    override def toString: String = HistoryStorageTemplateValue
  }

  def parse(string: String): CheckName = string match {
    case StorageRegistryValue => StorageRegistry
    case StorageTemplateValue => StorageTemplate
    case HistoryStorageTemplateValue => HistoryStorageTemplate
    case _ => throw new IllegalArgumentException
  }
}
