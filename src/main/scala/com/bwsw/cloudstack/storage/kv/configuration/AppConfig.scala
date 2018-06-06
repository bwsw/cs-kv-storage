package com.bwsw.cloudstack.storage.kv.configuration

import com.typesafe.config._

import scala.concurrent.duration._

class AppConfig {
  private val conf = ConfigFactory.load

  def getMaxCacheSize: Int = {
    conf.getInt("app.cache.max-size")
  }

  def getCacheExpirationTime: String = {
    conf.getString("app.cache.expiration-time")
  }

  def getFlushHistorySize: Int = {
    conf.getInt("app.history.flush-size")
  }

  def getFlushHistoryTimeout: FiniteDuration = {
    val timeout = Duration(conf.getString("app.history.flush-timeout"))
    timeout match {
      case f: FiniteDuration => f
      case _ => throw new RuntimeException("Misconfiguration in history.flush-timeout")
    }
  }

  def getHistoryRetryLimit: Int = {
    conf.getInt("app.history.retry-limit")
  }

  def getRequestTimeout: FiniteDuration = {
    val timeout = Duration(conf.getString("app.request-timeout"))
    timeout match {
      case f: FiniteDuration => f
      case _ => throw new RuntimeException("Misconfiguration in request-timeout")
    }
  }
}
