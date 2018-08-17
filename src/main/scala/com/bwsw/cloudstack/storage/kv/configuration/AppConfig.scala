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

import java.util.concurrent.TimeUnit

import com.typesafe.config._

import scala.concurrent.duration._

/** Provides access to application level configurations **/
class AppConfig {
  private val conf = ConfigFactory.load.getConfig("app")

  def getMaxCacheSize: Int = {
    conf.getInt("cache.max-size")
  }

  def getCacheExpirationTime: String = {
    conf.getString("cache.expiration-time")
  }

  def getCacheUpdateTime: FiniteDuration = {
    FiniteDuration(conf.getDuration("cache.update-time").toMillis, TimeUnit.MILLISECONDS)
  }

  def getFlushHistorySize: Int = {
    conf.getInt("history.flush-size")
  }

  def getFlushHistoryTimeout: FiniteDuration = {
    FiniteDuration(conf.getDuration("history.flush-timeout").toMillis, TimeUnit.MILLISECONDS)
  }

  def getHistoryRetryLimit: Int = {
    conf.getInt("history.retry-limit")
  }

  def getRequestTimeout: FiniteDuration = {
    FiniteDuration(conf.getDuration("request-timeout").toMillis, TimeUnit.MILLISECONDS)
  }

  def getDefaultPageSize: Int = {
    conf.getInt("default-page-size")
  }
}
