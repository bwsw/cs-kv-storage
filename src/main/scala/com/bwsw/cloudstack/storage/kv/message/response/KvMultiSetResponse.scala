package com.bwsw.cloudstack.storage.kv.message.response

import com.bwsw.cloudstack.storage.kv.entity.Storage
import com.bwsw.cloudstack.storage.kv.error.StorageError

case class KvMultiSetResponse(
                               storage: Storage,
                               kvs: Map[String, String],
                               timestamp: Long,
                               response: Either[StorageError, Map[String, Boolean]])
  extends KvResponse
