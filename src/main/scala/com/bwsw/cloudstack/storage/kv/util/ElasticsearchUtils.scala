package com.bwsw.cloudstack.storage.kv.util

import com.bwsw.cloudstack.storage.kv.error.InternalError
import com.sksamuel.elastic4s.http.RequestFailure

/** An utility for Elasticsearch operations **/
object ElasticsearchUtils {

  def getStorageIndex(storage: String): String = "storage-" + storage

  def getError(requestFailure: RequestFailure): InternalError = {
    if (requestFailure.error == null)
      InternalError("Elasticsearch error")
    else InternalError(requestFailure.error.reason)
  }
}
