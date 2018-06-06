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

package com.bwsw.cloudstack.storage.kv.app

import com.typesafe.config._

class Configuration {
  private val conf = ConfigFactory.load

  def getElasticsearchUri: String = {
    conf.getString("elasticsearch.uri")
  }

  def getElasticsearchUsername: String = {
    conf.getString("elasticsearch.auth.username")
  }

  def getElasticsearchPassword: String = {
    conf.getString("elasticsearch.auth.password")
  }

  def getSearchPageSize: Int = {
    conf.getInt("elasticsearch.search.pagesize")
  }

  def getSearchScrollKeepAlive: String = {
    conf.getString("elasticsearch.search.keepalive")
  }

  def getMaxValueLength: Int = {
    conf.getInt("elasticsearch.limit.max-value-length")
  }

  def getMaxKeyLength: Int = {
    conf.getInt("elasticsearch.limit.max-key-length")
  }
}
