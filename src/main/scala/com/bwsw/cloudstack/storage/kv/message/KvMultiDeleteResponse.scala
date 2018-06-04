package com.bwsw.cloudstack.storage.kv.message

import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvMultiDeleteResponse(storage: String, keys: Iterable[String], timestamp: Long, response: Either[StorageError, Map[String, Boolean]])
