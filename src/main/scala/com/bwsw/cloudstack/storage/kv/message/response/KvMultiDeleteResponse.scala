package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvMultiDeleteResponse(storage: String, timestamp: Long, response: Either[StorageError, Map[String, Boolean]])
