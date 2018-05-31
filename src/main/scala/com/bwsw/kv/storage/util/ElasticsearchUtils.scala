package com.bwsw.kv.storage.util

/** An utility for Elasticsearch operations **/
object ElasticsearchUtils {

  def getStorageIndex(storage: String): String = "storage-" + storage
}
