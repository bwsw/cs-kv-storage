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
