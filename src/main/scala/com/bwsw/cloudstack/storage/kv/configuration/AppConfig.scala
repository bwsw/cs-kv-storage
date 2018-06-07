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
}
