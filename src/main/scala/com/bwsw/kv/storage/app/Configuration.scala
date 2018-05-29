package com.bwsw.kv.storage.app

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
