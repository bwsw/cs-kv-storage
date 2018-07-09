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

package com.bwsw.cloudstack.storage.kv.configuration

import com.typesafe.config.ConfigFactory

/** Provides access to Elasticsearch specific configurations **/
class ElasticsearchConfig {
  private val conf = ConfigFactory.load.getConfig("elasticsearch")

  def getUri: String = {
    conf.getString("uri")
  }

  def getUsername: String = {
    conf.getString("auth.username")
  }

  def getPassword: String = {
    conf.getString("auth.password")
  }

  def getScrollPageSize: Int = {
    conf.getInt("scroll.page-size")
  }

  def getScrollKeepAlive: String = {
    conf.getString("scroll.keep-alive")
  }

  def getMaxValueLength: Int = {
    conf.getInt("limit.value.max-size")
  }

  def getMaxKeyLength: Int = {
    conf.getInt("limit.key.max-size")
  }
}
