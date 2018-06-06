package com.bwsw.cloudstack.storage.kv.configuration

import com.typesafe.config.ConfigFactory

class ElasticsearchConfig {
  private val conf = ConfigFactory.load

  def getUri: String = {
    conf.getString("elasticsearch.uri")
  }

  def getUsername: String = {
    conf.getString("elasticsearch.auth.username")
  }

  def getPassword: String = {
    conf.getString("elasticsearch.auth.password")
  }

  def getScrollPageSize: Int = {
    conf.getInt("elasticsearch.scroll.page-size")
  }

  def getScrollKeepAlive: String = {
    conf.getString("elasticsearch.scroll.keep-alive")
  }

  def getMaxValueLength: Int = {
    conf.getInt("elasticsearch.limit.value.max-size")
  }

  def getMaxKeyLength: Int = {
    conf.getInt("elasticsearch.limit.key.max-size")
  }
}
