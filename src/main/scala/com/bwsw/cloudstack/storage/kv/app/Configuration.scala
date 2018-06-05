package com.bwsw.cloudstack.storage.kv.app

import com.typesafe.config._

import scala.concurrent.duration._

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
    conf.getInt("elasticsearch.limit.max-value-size")
  }

  def getMaxKeyLength: Int = {
    conf.getInt("elasticsearch.limit.max-key-size")
  }

  def getMaxStorageCacheSize: Int = {
    conf.getInt("elasticsearch.cache.max-size")
  }

  def getStorageCacheExpirationTime: String = {
    conf.getString("elasticsearch.cache.expiration-time")
  }

  def getFlushHistorySize: Int = {
    conf.getInt("elasticsearch.history.flush-size")
  }

  def getFlushHistoryTimeout: FiniteDuration = {
    val timeout = Duration(conf.getString("elasticsearch.history.flush-timeout"))
    timeout match {
      case f: FiniteDuration => f
      case _ => throw new RuntimeException("Misconfiguration in elasticsearch.history.flush-timeout")
    }
  }

  def getRequestTimeout: FiniteDuration = {
    val timeout = Duration(conf.getString("elasticsearch.history.request-timeout"))
    timeout match {
      case f: FiniteDuration => f
      case _ => throw new RuntimeException("Misconfiguration in elasticsearch.history.request-timeout")
    }
  }

  def getHistoryRetryLimit: Int = {
    conf.getInt("elasticsearch.history.retry-limit")
  }
}
