package com.bwsw.kv.storage.models
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
}
