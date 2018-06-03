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
    conf.getInt("elasticsearch.limit.max-value-length")
  }

  def getMaxKeyLength: Int = {
    conf.getInt("elasticsearch.limit.max-key-length")
  }

  // TODO: extract values from the configuration file
  def getFlushHistorySize: Int = 1000

  def getFlushHistoryTimeout: FiniteDuration = 30.seconds

  def getRequestTimeout: FiniteDuration = 1.second
}
