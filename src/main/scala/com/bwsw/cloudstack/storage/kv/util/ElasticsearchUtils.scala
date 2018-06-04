package com.bwsw.cloudstack.storage.kv.util

/** An utility for Elasticsearch operations **/
object ElasticsearchUtils {

  def getStorageIndex(storage: String): String = "storage-" + storage
}
